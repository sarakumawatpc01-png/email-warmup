import os
import sqlite3
from typing import Optional
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Lock

import jwt
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import Index, String, create_engine, func, or_, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+pysqlite:///./lead.db")
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
JWT_ALGORITHM = "HS256"
AUTH_STATE_DB_PATH = os.getenv("AUTH_STATE_DB_PATH", "./auth_state.db")
SECRETS_MANAGER_URL = os.getenv("SECRETS_MANAGER_URL", "").rstrip("/")
SECRETS_MANAGER_TOKEN = os.getenv("SECRETS_MANAGER_TOKEN", "")
LEAD_BULK_RATE_LIMIT = int(os.getenv("LEAD_BULK_RATE_LIMIT", "20"))
LEAD_BULK_RATE_WINDOW_SECONDS = int(os.getenv("LEAD_BULK_RATE_WINDOW_SECONDS", "60"))
LEAD_SCHEMA_VERSION = "2026-03-31-lead-v1"
_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = defaultdict(deque)
_RATE_LIMIT_LOCK = Lock()

app = FastAPI(title="Lead Service", version="1.0.0")
logger = logging.getLogger("lead-service")


class Base(DeclarativeBase):
    pass


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    email: Mapped[str] = mapped_column(String(320), index=True)
    company: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    job_title: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(120))
    last_name: Mapped[Optional[str]] = mapped_column(String(120))

    __table_args__ = (
        Index("ix_leads_tenant_email", "tenant_id", "email", unique=True),
        Index("ix_leads_tenant_company", "tenant_id", "company"),
        Index("ix_leads_tenant_job_title", "tenant_id", "job_title"),
    )


def _resolve_secret(env_name: str, default: str) -> str:
    value = os.getenv(env_name)
    if value and not value.startswith("sm://"):
        return value
    secret_name = value.removeprefix("sm://") if value else env_name
    if not SECRETS_MANAGER_URL:
        return default
    try:
        import httpx

        headers = {}
        if SECRETS_MANAGER_TOKEN:
            headers["authorization"] = f"Bearer {SECRETS_MANAGER_TOKEN}"
        with httpx.Client(timeout=3) as client:
            response = client.get(f"{SECRETS_MANAGER_URL}/v1/secrets/{secret_name}", headers=headers)
        if response.status_code != 200:
            return default
        payload = response.json()
        secret_value = payload.get("value")
        return secret_value if isinstance(secret_value, str) and secret_value else default
    except Exception:
        logger.warning("secret resolution failed for %s", env_name)
        return default


def _is_rate_limit_allowed(scope: str, key: str, limit: int, window_seconds: int) -> bool:
    now = datetime.now(timezone.utc).timestamp()
    bucket_key = f"{scope}:{key.lower().strip()}"
    with _RATE_LIMIT_LOCK:
        bucket = _RATE_LIMIT_BUCKETS[bucket_key]
        while bucket and now - bucket[0] > window_seconds:
            bucket.popleft()
        if len(bucket) >= limit:
            return False
        bucket.append(now)
        return True


DATABASE_URL = _resolve_secret("DATABASE_URL", DATABASE_URL)
JWT_SECRET = _resolve_secret("JWT_SECRET", JWT_SECRET)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Base.metadata.create_all(engine)
with engine.begin() as conn:
    conn.exec_driver_sql(
        "CREATE TABLE IF NOT EXISTS schema_versions (service TEXT NOT NULL, version TEXT NOT NULL, applied_at TEXT NOT NULL, PRIMARY KEY(service, version))"
    )
    conn.exec_driver_sql(
        "INSERT OR IGNORE INTO schema_versions (service, version, applied_at) VALUES (?, ?, ?)",
        ("lead-service", LEAD_SCHEMA_VERSION, datetime.now(timezone.utc).isoformat()),
    )


class LeadCreate(BaseModel):
    email: EmailStr
    company: Optional[str] = Field(default=None, max_length=255)
    job_title: Optional[str] = Field(default=None, max_length=255)
    first_name: Optional[str] = Field(default=None, max_length=120)
    last_name: Optional[str] = Field(default=None, max_length=120)


class LeadOut(BaseModel):
    id: int
    tenant_id: str
    email: EmailStr
    company: Optional[str]
    job_title: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]

    model_config = {"from_attributes": True}


def parse_token(authorization: str = Header(default="")) -> dict:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        claims = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    if _is_revoked(claims):
        raise HTTPException(status_code=401, detail="Token revoked")
    if not claims.get("tenant_id"):
        raise HTTPException(status_code=401, detail="Tenant missing")
    return claims


def _is_revoked(claims: dict) -> bool:
    sid = claims.get("sid")
    jti = claims.get("jti")
    if not isinstance(sid, str) or not isinstance(jti, str):
        return False
    try:
        with sqlite3.connect(AUTH_STATE_DB_PATH) as conn:
            if isinstance(sid, str):
                row = conn.execute("SELECT 1 FROM revoked_sessions WHERE sid = ? LIMIT 1", (sid,)).fetchone()
                if row:
                    return True
            if isinstance(jti, str):
                row = conn.execute("SELECT 1 FROM revoked_tokens WHERE jti = ? LIMIT 1", (jti,)).fetchone()
                if row:
                    return True
    except sqlite3.Error:
        logger.warning("auth_state_db_unavailable", exc_info=True)
        return True
    return False


def _has_any_permission(claims: dict, *needed: str) -> bool:
    permissions = claims.get("permissions") or []
    if not isinstance(permissions, list):
        return False
    return "*" in permissions or any(item in permissions for item in needed)


def _require_warmup_access(claims: dict) -> None:
    if not _has_any_permission(claims, "warmup:read", "warmup:admin"):
        raise HTTPException(status_code=403, detail="Permission denied")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "lead-service"}


@app.post("/leads", response_model=LeadOut)
def create_lead(payload: LeadCreate, claims: dict = Depends(parse_token)) -> LeadOut:
    _require_warmup_access(claims)
    tenant_id = claims["tenant_id"]
    with Session(engine) as session:
        existing = session.scalar(
            select(Lead).where(Lead.tenant_id == tenant_id, Lead.email == payload.email.lower())
        )
        if existing:
            raise HTTPException(status_code=409, detail="Lead already exists")

        lead = Lead(
            tenant_id=tenant_id,
            email=payload.email.lower(),
            company=payload.company,
            job_title=payload.job_title,
            first_name=payload.first_name,
            last_name=payload.last_name,
        )
        session.add(lead)
        session.commit()
        session.refresh(lead)
        return LeadOut.model_validate(lead)


@app.get("/leads")
def list_leads(
    claims: dict = Depends(parse_token),
    company: Optional[str] = Query(default=None),
    job_title: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
) -> dict:
    _require_warmup_access(claims)
    tenant_id = claims["tenant_id"]
    with Session(engine) as session:
        stmt = select(Lead).where(Lead.tenant_id == tenant_id)
        if company:
            stmt = stmt.where(Lead.company == company)
        if job_title:
            stmt = stmt.where(Lead.job_title == job_title)
        if q:
            pattern = f"{q}%"
            stmt = stmt.where(
                or_(
                    Lead.email.ilike(pattern),
                    func.coalesce(Lead.company, "").ilike(pattern),
                    func.coalesce(Lead.job_title, "").ilike(pattern),
                )
            )

        total_stmt = select(func.count()).select_from(stmt.subquery())
        total = session.scalar(total_stmt) or 0
        rows = list(session.scalars(stmt.offset((page - 1) * page_size).limit(page_size)))

        return {
            "items": [
                {
                    "id": row.id,
                    "tenant_id": row.tenant_id,
                    "email": row.email,
                    "company": row.company,
                    "job_title": row.job_title,
                    "first_name": row.first_name,
                    "last_name": row.last_name,
                }
                for row in rows
            ],
            "page": page,
            "page_size": page_size,
            "total": total,
        }


@app.post("/leads/bulk")
def bulk_create(items: list[LeadCreate], claims: dict = Depends(parse_token)) -> dict:
    _require_warmup_access(claims)
    tenant_id = claims["tenant_id"]
    if not _is_rate_limit_allowed("bulk", tenant_id, LEAD_BULK_RATE_LIMIT, LEAD_BULK_RATE_WINDOW_SECONDS):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    normalized_emails = [item.email.lower() for item in items]
    unique_emails = list(dict.fromkeys(normalized_emails))
    created = 0
    rejected = 0

    with Session(engine) as session:
        existing_emails = set(
            session.scalars(
                select(Lead.email).where(Lead.tenant_id == tenant_id, Lead.email.in_(unique_emails))
            )
        )
        seen_new = set()
        to_create = []
        for item in items:
            email = item.email.lower()
            if email in existing_emails or email in seen_new:
                rejected += 1
                continue
            seen_new.add(email)
            to_create.append(
                Lead(
                    tenant_id=tenant_id,
                    email=email,
                    company=item.company,
                    job_title=item.job_title,
                    first_name=item.first_name,
                    last_name=item.last_name,
                )
            )

        if to_create:
            session.add_all(to_create)
            session.commit()
            created = len(to_create)
    return {"created": created, "rejected": rejected}
