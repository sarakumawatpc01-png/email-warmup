import os
from typing import Optional

import jwt
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import Index, String, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+pysqlite:///./lead.db")
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
JWT_ALGORITHM = "HS256"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
app = FastAPI(title="Lead Service", version="1.0.0")


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


Base.metadata.create_all(engine)


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


def parse_token(authorization: str = Header(default="")) -> dict:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        claims = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    if not claims.get("tenant_id"):
        raise HTTPException(status_code=401, detail="Tenant missing")
    return claims


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "lead-service"}


@app.post("/leads", response_model=LeadOut)
def create_lead(payload: LeadCreate, claims: dict = Depends(parse_token)) -> LeadOut:
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
        return LeadOut.model_validate(
            {
                "id": lead.id,
                "tenant_id": lead.tenant_id,
                "email": lead.email,
                "company": lead.company,
                "job_title": lead.job_title,
                "first_name": lead.first_name,
                "last_name": lead.last_name,
            }
        )


@app.get("/leads")
def list_leads(
    claims: dict = Depends(parse_token),
    company: Optional[str] = Query(default=None),
    job_title: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
) -> dict:
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
                Lead.email.ilike(pattern) | Lead.company.ilike(pattern) | Lead.job_title.ilike(pattern)
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
    created = 0
    rejected = 0
    for item in items:
        try:
            create_lead(item, claims)
            created += 1
        except HTTPException as exc:
            if exc.status_code == 409:
                rejected += 1
            else:
                raise
    return {"created": created, "rejected": rejected}
