from __future__ import annotations

import json
import logging
import math
import os
import random
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import jwt
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import Boolean, DateTime, Float, Index, Integer, String, create_engine, delete, func, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
except Exception:  # pragma: no cover - optional runtime capability
    trace = None
    OTLPSpanExporter = None
    FastAPIInstrumentor = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    ConsoleSpanExporter = None

try:
    import dns.resolver
except Exception:  # pragma: no cover - optional runtime capability
    dns = None
else:  # pragma: no cover
    dns = dns.resolver

try:
    from redis import Redis
    from rq import Queue, SimpleWorker
except Exception:  # pragma: no cover - optional runtime capability
    Redis = None
    Queue = None
    SimpleWorker = None

app = FastAPI(title="Warmup Engine", version="2.0.0")

DATABASE_URL = os.getenv("WARMUP_DATABASE_URL", "postgresql+psycopg2://email_saas:email_saas@postgres:5432/email_saas")
SQLITE_FALLBACK_URL = os.getenv("WARMUP_SQLITE_FALLBACK_URL", "sqlite+pysqlite:///./warmup.db")
EWMA_ALPHA = float(os.getenv("WARMUP_EWMA_ALPHA", "0.35"))
MAX_MAILBOX_DAILY_CAP = int(os.getenv("MAX_MAILBOX_DAILY_CAP", "250"))
MAX_TENANT_DAILY_CAP = int(os.getenv("MAX_TENANT_DAILY_CAP", "5000"))
REDIS_URL = os.getenv("REDIS_URL", "")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
OTEL_ENABLE_CONSOLE_EXPORTER = os.getenv("OTEL_ENABLE_CONSOLE_EXPORTER", "false").lower() == "true"
PROMETHEUS_ENABLED = os.getenv("PROMETHEUS_ENABLED", "true").lower() == "true"
DEPLOY_ENV = os.getenv("DEPLOY_ENV", "dev")
SERVICE_NAME = "warmup-engine"
ADMIN_API_KEY = os.getenv("WARMUP_ADMIN_API_KEY", "")
AUTH_JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
AUTH_JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

def init_engine():
    try:
        candidate = create_engine(DATABASE_URL, pool_pre_ping=True)
        with candidate.connect() as conn:
            conn.execute(text("SELECT 1"))
        return candidate
    except Exception:
        return create_engine(SQLITE_FALLBACK_URL, pool_pre_ping=True)


engine = init_engine()

logger = logging.getLogger("warmup-engine")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

METRICS: dict[str, int] = defaultdict(int)
IN_MEMORY_QUEUES: dict[str, deque] = defaultdict(deque)
DEAD_LETTER_QUEUE: deque = deque(maxlen=1000)
IN_MEMORY_INFLIGHT: dict[str, dict[str, Any]] = {}
GLOBAL_KILL_SWITCH = False
TENANT_KILL_SWITCHES: set[str] = set()
PROVIDER_KILL_SWITCHES: set[str] = set()
QUEUE_VISIBILITY_TIMEOUT_SECONDS = int(os.getenv("WARMUP_QUEUE_VISIBILITY_TIMEOUT_SECONDS", "45"))

QUEUE_NAMES = {"schedule_generation", "send_execution", "reply_simulation", "reputation_scoring"}

PROVIDER_PROFILES: dict[str, dict[str, float]] = {
    "gmail.com": {
        "kp": 0.30,
        "ki": 0.07,
        "kd": 0.15,
        "quality_setpoint": 0.68,
        "spam_soft_threshold": 0.035,
        "bounce_soft_threshold": 0.022,
        "spam_risk_trigger": 0.42,
        "quarantine_risk_trigger": 0.60,
        "max_step": 26.0,
    },
    "outlook.com": {
        "kp": 0.33,
        "ki": 0.08,
        "kd": 0.17,
        "quality_setpoint": 0.66,
        "spam_soft_threshold": 0.040,
        "bounce_soft_threshold": 0.026,
        "spam_risk_trigger": 0.43,
        "quarantine_risk_trigger": 0.61,
        "max_step": 28.0,
    },
    "yahoo.com": {
        "kp": 0.28,
        "ki": 0.06,
        "kd": 0.14,
        "quality_setpoint": 0.67,
        "spam_soft_threshold": 0.038,
        "bounce_soft_threshold": 0.025,
        "spam_risk_trigger": 0.41,
        "quarantine_risk_trigger": 0.59,
        "max_step": 24.0,
    },
    "default": {
        "kp": 0.35,
        "ki": 0.08,
        "kd": 0.18,
        "quality_setpoint": 0.65,
        "spam_soft_threshold": 0.045,
        "bounce_soft_threshold": 0.03,
        "spam_risk_trigger": 0.4,
        "quarantine_risk_trigger": 0.6,
        "max_step": 35.0,
    },
}

PROM_HTTP_REQUESTS_TOTAL = Counter(
    "warmup_http_requests_total",
    "Total warmup-engine HTTP requests",
    ["service", "env", "method", "path", "status_code"],
)
PROM_HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "warmup_http_request_duration_seconds",
    "Warmup-engine HTTP request duration in seconds",
    ["service", "env", "method", "path"],
)
PROM_QUEUE_BACKLOG = Gauge(
    "warmup_queue_backlog",
    "Current warmup queue backlog depth",
    ["service", "env", "queue_name", "backend"],
)
PROM_SLO_SEND_SUCCESS_RATIO = Gauge(
    "warmup_slo_send_success_ratio",
    "SLO gauge for send success ratio",
    ["service", "env", "provider"],
)
PROM_SLO_PLACEMENT_SCORE = Gauge(
    "warmup_slo_placement_score",
    "SLO gauge for inbox placement score",
    ["service", "env", "provider"],
)
PROM_SLO_QUEUE_LATENCY_SECONDS = Gauge(
    "warmup_slo_queue_latency_seconds",
    "SLO gauge for queue latency proxy in seconds",
    ["service", "env", "queue_name", "backend"],
)


class Base(DeclarativeBase):
    pass


class MailboxProfile(Base):
    __tablename__ = "mailbox_profiles"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    mailbox: Mapped[str] = mapped_column(String(320), index=True)
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    domain_age_days: Mapped[int] = mapped_column(Integer, default=0)

    inbox_ewma: Mapped[float] = mapped_column(Float, default=0.72)
    spam_ewma: Mapped[float] = mapped_column(Float, default=0.07)
    bounce_ewma: Mapped[float] = mapped_column(Float, default=0.03)
    complaint_ewma: Mapped[float] = mapped_column(Float, default=0.004)
    reply_ewma: Mapped[float] = mapped_column(Float, default=0.16)

    reputation_score: Mapped[float] = mapped_column(Float, default=0.65)
    risk_score: Mapped[float] = mapped_column(Float, default=0.2)
    mode: Mapped[str] = mapped_column(String(32), default="normal")
    current_daily_target: Mapped[int] = mapped_column(Integer, default=10)

    pid_integral: Mapped[float] = mapped_column(Float, default=0.0)
    pid_prev_error: Mapped[float] = mapped_column(Float, default=0.0)
    stable_windows: Mapped[int] = mapped_column(Integer, default=0)
    blacklisted: Mapped[bool] = mapped_column(Boolean, default=False)
    partner_histogram: Mapped[str] = mapped_column(String, default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_mailbox_profiles_tenant_mailbox", "tenant_id", "mailbox", unique=True),
    )


class WarmupJob(Base):
    __tablename__ = "warmup_jobs"

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    mailbox: Mapped[str] = mapped_column(String(320), index=True)
    status: Mapped[str] = mapped_column(String(32), default="active")
    daily_target: Mapped[int] = mapped_column(Integer)
    interval_minutes: Mapped[int] = mapped_column(Integer)
    reply_simulation_rate: Mapped[float] = mapped_column(Float)
    spam_rescue_rate: Mapped[float] = mapped_column(Float)
    next_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_warmup_jobs_tenant_mailbox", "tenant_id", "mailbox"),
    )


class WarmupEvent(Base):
    __tablename__ = "warmup_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    mailbox: Mapped[str] = mapped_column(String(320), index=True)
    event_type: Mapped[str] = mapped_column(String(64))
    outcome: Mapped[str] = mapped_column(String(64), default="accepted")
    queue_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True)
    status: Mapped[str] = mapped_column(String(32), default="queued")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    month_bucket: Mapped[str] = mapped_column(String(7), default=lambda: datetime.now(timezone.utc).strftime("%Y-%m"))
    payload: Mapped[str] = mapped_column(String, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_warmup_events_tenant_mailbox", "tenant_id", "mailbox"),
        Index("ix_warmup_events_month_bucket", "month_bucket"),
    )


class DeliverabilitySnapshot(Base):
    __tablename__ = "deliverability_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    mailbox: Mapped[str] = mapped_column(String(320), index=True)
    domain: Mapped[str] = mapped_column(String(255), index=True)
    spf_aligned: Mapped[bool] = mapped_column(Boolean, default=False)
    dkim_aligned: Mapped[bool] = mapped_column(Boolean, default=False)
    dmarc_aligned: Mapped[bool] = mapped_column(Boolean, default=False)
    ptr_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    tls_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    inbox_pct: Mapped[float] = mapped_column(Float, default=0.0)
    promotions_pct: Mapped[float] = mapped_column(Float, default=0.0)
    spam_pct: Mapped[float] = mapped_column(Float, default=0.0)
    score: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_deliverability_snapshots_tenant_mailbox", "tenant_id", "mailbox"),
    )


class RiskSignal(Base):
    __tablename__ = "risk_signals"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    mailbox: Mapped[str] = mapped_column(String(320), index=True)
    signal_type: Mapped[str] = mapped_column(String(64))
    severity: Mapped[float] = mapped_column(Float)
    details: Mapped[str] = mapped_column(String, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_risk_signals_tenant_mailbox", "tenant_id", "mailbox"),
    )


class InternalMailbox(Base):
    __tablename__ = "internal_mailboxes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    mailbox: Mapped[str] = mapped_column(String(320), index=True)
    provider: Mapped[str] = mapped_column(String(120))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[str] = mapped_column(String, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_internal_mailboxes_tenant_mailbox", "tenant_id", "mailbox", unique=True),
    )


class AdminAuditLog(Base):
    __tablename__ = "admin_audit_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(120), index=True)
    action: Mapped[str] = mapped_column(String(120), index=True)
    resource_type: Mapped[str] = mapped_column(String(120))
    resource_id: Mapped[str] = mapped_column(String(255), default="")
    details: Mapped[str] = mapped_column(String, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(120), index=True)
    dedupe_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    payload: Mapped[str] = mapped_column(String, default="{}")
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str] = mapped_column(String, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_outbox_events_status_created", "status", "created_at"),
    )


class InboxEvent(Base):
    __tablename__ = "inbox_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(120), index=True)
    message_id: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    payload: Mapped[str] = mapped_column(String, default="{}")
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

    __table_args__ = (
        Index("ix_inbox_events_source_processed", "source", "processed_at"),
    )


class WorkerLease(Base):
    __tablename__ = "worker_leases"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(Integer, index=True)
    queue_name: Mapped[str] = mapped_column(String(64), index=True)
    lease_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_worker_leases_event_queue", "event_id", "queue_name", unique=True),
    )


Base.metadata.create_all(engine)

if trace and TracerProvider and BatchSpanProcessor:
    resource = Resource.create({"service.name": "warmup-engine"}) if Resource else None
    provider = TracerProvider(resource=resource)
    if OTEL_EXPORTER_OTLP_ENDPOINT and OTLPSpanExporter:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTEL_EXPORTER_OTLP_ENDPOINT.rstrip('/')}/v1/traces"))
        )
    elif OTEL_ENABLE_CONSOLE_EXPORTER and ConsoleSpanExporter:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    if FastAPIInstrumentor:
        FastAPIInstrumentor.instrument_app(app)

TRACER = trace.get_tracer("warmup-engine") if trace else None


class WarmupJobRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    domain_age_days: int = Field(ge=0)
    blacklist_detected: bool = False
    timezone: str = "UTC"


class ReputationUpdateRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    inbox_rate: float = Field(ge=0.0, le=1.0)
    spam_rate: float = Field(ge=0.0, le=1.0)
    bounce_rate: float = Field(ge=0.0, le=1.0)
    complaint_rate: float = Field(ge=0.0, le=1.0)
    reply_rate: float = Field(ge=0.0, le=1.0)
    blacklist_detected: bool = False


class ScheduleRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    partner_pool: list[str] = Field(default_factory=list)
    requested_count: int = Field(default=20, ge=1, le=250)


class QueueTaskRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    queue_name: Literal["schedule_generation", "send_execution", "reply_simulation", "reputation_scoring"]
    idempotency_key: str = Field(min_length=8, max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=4, ge=1, le=10)


class DeliverabilityCheckRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    domain: str
    dkim_selector: str = "default"
    ptr_valid: bool | None = None
    tls_supported: bool | None = None
    inbox_pct: float = Field(default=0.7, ge=0.0, le=1.0)
    promotions_pct: float = Field(default=0.2, ge=0.0, le=1.0)
    spam_pct: float = Field(default=0.1, ge=0.0, le=1.0)


class ContentPlanRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    day_number: int = Field(ge=1, le=365)


class KillSwitchRequest(BaseModel):
    scope: Literal["global", "tenant", "provider"]
    enabled: bool
    value: str | None = None


class AbuseCheckRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    complaint_spike: bool = False
    repeated_bad_domains: int = Field(default=0, ge=0)
    anomalous_burstiness: float = Field(default=0.0, ge=0.0, le=1.0)


class InternalMailboxRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    notes: str = ""


class DlqReplayRequest(BaseModel):
    item_index: int = Field(ge=0)
    approved_by: str = Field(min_length=2, max_length=120)
    reason: str = Field(min_length=3, max_length=255)


class LeaseRenewRequest(BaseModel):
    event_id: int = Field(ge=1)
    queue_name: Literal["schedule_generation", "send_execution", "reply_simulation", "reputation_scoring"]
    extend_seconds: int = Field(default=30, ge=5, le=300)


class InboxRecordRequest(BaseModel):
    source: str = Field(min_length=2, max_length=120)
    message_id: str = Field(min_length=8, max_length=160)
    payload: dict[str, Any] = Field(default_factory=dict)


def provider_profile_for_mailbox(mailbox: str) -> dict[str, float]:
    provider = provider_from_mailbox(mailbox)
    return PROVIDER_PROFILES.get(provider, PROVIDER_PROFILES["default"])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def ewma(previous: float, observed: float, alpha: float = EWMA_ALPHA) -> float:
    return alpha * observed + (1 - alpha) * previous


def provider_from_mailbox(mailbox: str) -> str:
    domain = mailbox.split("@")[-1].lower()
    return domain


def json_log(event: str, **kwargs: Any) -> None:
    logger.info(json.dumps({"event": event, **kwargs}, default=str))


def extract_token_claims(authorization: str | None) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        claims = jwt.decode(token, AUTH_JWT_SECRET, algorithms=[AUTH_JWT_ALGORITHM])
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    return claims if isinstance(claims, dict) else {}


def _has_permission(claims: dict[str, Any], permission: str) -> bool:
    permissions = claims.get("permissions") or []
    if not isinstance(permissions, list):
        return False
    return "*" in permissions or permission in permissions


def require_admin(
    api_key: str | None,
    authorization: str | None = None,
    *,
    permission: str = "warmup:admin",
    tenant_scope: str | None = None,
) -> dict[str, Any]:
    claims: dict[str, Any] = {}
    if authorization:
        claims = extract_token_claims(authorization)
        if not _has_permission(claims, permission):
            raise HTTPException(status_code=403, detail="Permission denied")
        if tenant_scope and claims.get("role") != "superadmin" and claims.get("tenant_id") != tenant_scope:
            raise HTTPException(status_code=403, detail="Tenant scope denied")
        return claims
    if not ADMIN_API_KEY:
        return claims
    if not api_key or api_key != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Admin API key required")
    return claims


def write_admin_audit_log(
    session: Session,
    *,
    actor: str,
    action: str,
    resource_type: str,
    resource_id: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    session.add(
        AdminAuditLog(
            actor=actor,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=json.dumps(details or {}),
        )
    )


def ensure_outbox_event(session: Session, *, topic: str, dedupe_key: str, payload: dict[str, Any]) -> OutboxEvent:
    existing = session.scalar(select(OutboxEvent).where(OutboxEvent.dedupe_key == dedupe_key))
    if existing:
        return existing
    event = OutboxEvent(topic=topic, dedupe_key=dedupe_key, payload=json.dumps(payload), status="pending")
    session.add(event)
    session.flush()
    return event


def mark_outbox_dispatched(session: Session, dedupe_key: str) -> bool:
    event = session.scalar(select(OutboxEvent).where(OutboxEvent.dedupe_key == dedupe_key))
    if not event:
        return False
    if event.status == "dispatched":
        return True
    event.status = "dispatched"
    event.dispatched_at = utc_now()
    return True


def base_target_from_domain_age(domain_age_days: int) -> int:
    ramp_factor = min(1.0, max(0.1, domain_age_days / 120))
    return int(5 + 95 * ramp_factor)


def load_profile_histogram(profile: MailboxProfile) -> dict[str, int]:
    try:
        data = json.loads(profile.partner_histogram or "{}")
        return {str(k): int(v) for k, v in data.items()}
    except Exception:
        return {}


def store_profile_histogram(profile: MailboxProfile, histogram: dict[str, int]) -> None:
    profile.partner_histogram = json.dumps(histogram)


def entropy_score(histogram: dict[str, int]) -> float:
    total = sum(histogram.values())
    if total <= 0:
        return 1.0
    entropy = 0.0
    for count in histogram.values():
        if count <= 0:
            continue
        p = count / total
        entropy -= p * math.log2(p)
    max_entropy = math.log2(max(2, len(histogram)))
    if max_entropy == 0:
        return 1.0
    return clamp(entropy / max_entropy, 0.0, 1.0)


def choose_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def window_ranges_for_day(local_day: datetime) -> list[tuple[int, int]]:
    if local_day.weekday() >= 5:  # weekend profile
        return [(9, 11), (16, 18)]
    return [(8, 11), (14, 18)]


def generate_humanized_schedule(profile: MailboxProfile, partners: list[str], count: int) -> list[dict[str, Any]]:
    tz = choose_timezone(profile.timezone)
    local_now = utc_now().astimezone(tz)
    ranges = window_ranges_for_day(local_now)
    rng_seed = f"{profile.tenant_id}:{profile.mailbox}:{local_now.date().isoformat()}:{count}:{len(partners)}"
    rng = random.Random(rng_seed)

    events = []
    for i in range(count):
        start_h, end_h = ranges[i % len(ranges)]
        hour = rng.randint(start_h, max(start_h, end_h - 1))
        minute = rng.randint(0, 59)
        offset_days = 0 if hour >= local_now.hour else 1

        local_send = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=offset_days)
        partner = partners[i % len(partners)] if partners else "seed@warmup.local"

        thread_length = rng.randint(1, 4)
        reply_delay_minutes = rng.randint(7, 240)
        events.append(
            {
                "send_at": local_send.astimezone(timezone.utc).isoformat(),
                "partner": partner,
                "thread_length": thread_length,
                "reply_delay_minutes": reply_delay_minutes,
                "include_link": False,
                "include_image": False,
            }
        )
    return sorted(events, key=lambda item: item["send_at"])


def pick_partners(profile: MailboxProfile, pool: list[str], requested_count: int) -> tuple[list[str], float]:
    unique_pool = [mail for mail in dict.fromkeys(pool) if mail and mail.lower() != profile.mailbox.lower()]
    if not unique_pool:
        unique_pool = [f"seed-{i}@warmup.local" for i in range(1, 7)]

    histogram = load_profile_histogram(profile)
    selected: list[str] = []
    last_partner = None

    for _ in range(requested_count):
        weighted: list[tuple[str, float]] = []
        for candidate in unique_pool:
            seen_count = histogram.get(candidate, 0)
            weight = 1 / (1 + seen_count)
            if candidate == last_partner:
                weight *= 0.2  # reduce immediate reselection probability for the same partner
            weighted.append((candidate, weight))

        total_weight = sum(weight for _, weight in weighted)
        roll = random.random() * total_weight
        upto = 0.0
        chosen = weighted[0][0]
        for candidate, weight in weighted:
            upto += weight
            if upto >= roll:
                chosen = candidate
                break

        selected.append(chosen)
        histogram[chosen] = histogram.get(chosen, 0) + 1
        last_partner = chosen

    score = entropy_score(histogram)
    store_profile_histogram(profile, histogram)
    return selected, score


def get_or_create_profile(session: Session, tenant_id: str, mailbox: str, domain_age_days: int = 0, timezone_name: str = "UTC") -> MailboxProfile:
    profile = session.scalar(
        select(MailboxProfile).where(MailboxProfile.tenant_id == tenant_id, MailboxProfile.mailbox == mailbox.lower())
    )
    if profile:
        return profile

    profile = MailboxProfile(
        tenant_id=tenant_id,
        mailbox=mailbox.lower(),
        timezone=timezone_name,
        domain_age_days=domain_age_days,
        current_daily_target=base_target_from_domain_age(domain_age_days),
    )
    session.add(profile)
    session.flush()
    return profile


def aggregate_tenant_daily_target(session: Session, tenant_id: str) -> int:
    return int(
        session.scalar(select(func.coalesce(func.sum(WarmupJob.daily_target), 0)).where(WarmupJob.tenant_id == tenant_id))
        or 0
    )


def classify_risk(profile: MailboxProfile, blacklist_detected: bool) -> float:
    structural = 0.0
    if blacklist_detected:
        structural += 0.35
    base = (
        profile.spam_ewma * 0.34
        + profile.bounce_ewma * 0.24
        + profile.complaint_ewma * 0.26
        + max(0.0, 0.18 - profile.reply_ewma) * 0.20
    )
    return clamp(base + structural, 0.0, 1.0)


def quality_score(profile: MailboxProfile) -> float:
    return clamp(
        profile.inbox_ewma * 0.55
        + profile.reply_ewma * 0.2
        - profile.spam_ewma * 0.35
        - profile.bounce_ewma * 0.25
        - profile.complaint_ewma * 0.4,
        0.0,
        1.0,
    )


def apply_adaptive_policy(profile: MailboxProfile, risk: float, blacklist_detected: bool) -> tuple[int, str]:
    profile_cfg = provider_profile_for_mailbox(profile.mailbox)
    kp, ki, kd = profile_cfg["kp"], profile_cfg["ki"], profile_cfg["kd"]
    quality_setpoint = profile_cfg["quality_setpoint"]
    spam_soft_threshold = profile_cfg["spam_soft_threshold"]
    bounce_soft_threshold = profile_cfg["bounce_soft_threshold"]
    spam_risk_trigger = profile_cfg["spam_risk_trigger"]
    quarantine_risk_trigger = profile_cfg["quarantine_risk_trigger"]
    max_step = profile_cfg["max_step"]

    quality = quality_score(profile)
    error = quality - quality_setpoint

    profile.pid_integral = clamp(profile.pid_integral + error, -5.0, 5.0)
    derivative = error - profile.pid_prev_error
    profile.pid_prev_error = error

    delta = kp * error + ki * profile.pid_integral + kd * derivative
    candidate = int(round(profile.current_daily_target + (delta * max_step)))
    candidate = int(clamp(candidate, 1, MAX_MAILBOX_DAILY_CAP))

    mode = profile.mode
    sender_pressure_high = profile.spam_ewma >= spam_soft_threshold or profile.bounce_ewma >= bounce_soft_threshold
    if blacklist_detected or risk >= quarantine_risk_trigger:
        mode = "quarantine"
        candidate = min(candidate, 5)
        profile.stable_windows = 0
    elif risk >= spam_risk_trigger or sender_pressure_high:
        mode = "throttle"
        candidate = min(candidate, max(10, int(profile.current_daily_target * 0.7)))
        profile.stable_windows = 0
    elif risk <= 0.17 and profile.mode in {"quarantine", "rescue", "throttle"}:
        profile.stable_windows += 1
        if profile.stable_windows >= 3:
            mode = "normal"
        else:
            mode = "rescue"
            candidate = max(candidate, 8)
    else:
        if mode in {"quarantine", "rescue", "throttle"} and risk < 0.25:
            mode = "rescue"
        elif risk < 0.25:
            mode = "normal"

    return candidate, mode


def is_killed(tenant_id: str, mailbox: str) -> bool:
    if GLOBAL_KILL_SWITCH:
        return True
    if tenant_id in TENANT_KILL_SWITCHES:
        return True
    provider = provider_from_mailbox(mailbox)
    return provider in PROVIDER_KILL_SWITCHES


def maybe_resolve_dns_txt(name: str) -> str:
    if dns is None:
        return ""
    try:
        answers = dns.resolve(name, "TXT")
        return " ".join(str(r).strip('"') for r in answers)
    except Exception:
        return ""


def execute_queue_task(payload: dict[str, Any]) -> dict[str, Any]:
    queue_name = payload.get("queue_name", "")
    if queue_name not in QUEUE_NAMES:
        raise ValueError("unsupported queue")

    if payload.get("force_fail"):
        raise RuntimeError("forced failure")

    return {"status": "processed", "queue": queue_name, "processed_at": utc_now().isoformat()}


def refresh_slo_metrics() -> None:
    processed = METRICS.get("queue_processed", 0)
    failed = METRICS.get("queue_dead_letter", 0)
    total = processed + failed
    send_success_ratio = (processed / total) if total else 1.0
    PROM_SLO_SEND_SUCCESS_RATIO.labels(service=SERVICE_NAME, env=DEPLOY_ENV, provider="all").set(send_success_ratio)

    placement_score = max(0.0, 1.0 - (METRICS.get("deliverability_spam_events", 0) / max(1, METRICS.get("deliverability_checks", 1))))
    PROM_SLO_PLACEMENT_SCORE.labels(service=SERVICE_NAME, env=DEPLOY_ENV, provider="all").set(placement_score)

    retry_events = METRICS.get("queue_retried", 0)
    for queue_name in QUEUE_NAMES:
        PROM_SLO_QUEUE_LATENCY_SECONDS.labels(
            service=SERVICE_NAME, env=DEPLOY_ENV, queue_name=queue_name, backend="in-memory"
        ).set(float(retry_events))


def refresh_queue_backlog_metrics() -> None:
    for queue_name in QUEUE_NAMES:
        PROM_QUEUE_BACKLOG.labels(
            service=SERVICE_NAME, env=DEPLOY_ENV, queue_name=queue_name, backend="in-memory"
        ).set(len(IN_MEMORY_QUEUES[queue_name]))


def _event_key(queue_name: str, event_id: Any) -> str:
    return f"{queue_name}:{event_id}"


def sweep_stuck_inflight_tasks() -> dict[str, int]:
    now = utc_now()
    moved = 0
    dead_lettered = 0
    with Session(engine) as session:
        leases = list(session.scalars(select(WorkerLease).where(WorkerLease.lease_until <= now).limit(500)))
        for lease in leases:
            event = session.get(WarmupEvent, lease.event_id)
            if event is None:
                session.delete(lease)
                IN_MEMORY_INFLIGHT.pop(_event_key(lease.queue_name, lease.event_id), None)
                continue

            queue_name = lease.queue_name or event.queue_name or "send_execution"
            if queue_name not in QUEUE_NAMES:
                session.delete(lease)
                IN_MEMORY_INFLIGHT.pop(_event_key(queue_name, lease.event_id), None)
                continue

            try:
                payload = json.loads(event.payload or "{}")
            except Exception:
                payload = {}
            attempts = max(1, int(event.retry_count) + 1)
            max_attempts = int(payload.get("max_attempts", 4))
            task = {
                "event_id": event.id,
                "queue_name": queue_name,
                "tenant_id": event.tenant_id,
                "mailbox": event.mailbox,
                "idempotency_key": event.idempotency_key,
                "attempt": attempts,
                "max_attempts": max_attempts,
                **payload,
            }
            if attempts >= max_attempts:
                DEAD_LETTER_QUEUE.append({"task": task, "error": "lease_timeout", "failed_at": now.isoformat()})
                event.status = "dead_letter"
                event.outcome = "failed"
                METRICS["queue_dead_letter"] += 1
                dead_lettered += 1
            else:
                task["attempt"] = attempts + 1
                task["retry_backoff_seconds"] = min(60, 2 ** attempts)
                IN_MEMORY_QUEUES[queue_name].append(task)
                event.retry_count += 1
                event.status = "retrying"
                METRICS["queue_retried"] += 1
                moved += 1
            session.delete(lease)
            IN_MEMORY_INFLIGHT.pop(_event_key(queue_name, event.id), None)
        session.commit()
    refresh_slo_metrics()
    refresh_queue_backlog_metrics()
    return {"requeued": moved, "dead_lettered": dead_lettered}


def get_rq_queue(queue_name: str):
    if not (REDIS_URL and Redis and Queue):
        return None
    conn = Redis.from_url(REDIS_URL)
    return Queue(queue_name, connection=conn)


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "service": "warmup-engine",
        "redis_enabled": bool(REDIS_URL and Redis and Queue),
        "queues": sorted(QUEUE_NAMES),
    }


@app.get("/metrics")
def metrics() -> dict:
    return {
        "counters": dict(METRICS),
        "dead_letter_queue_size": len(DEAD_LETTER_QUEUE),
        "inflight_queue_size": len(IN_MEMORY_INFLIGHT),
    }


@app.get("/metrics/prometheus")
def metrics_prometheus() -> Response:
    if not PROMETHEUS_ENABLED:
        raise HTTPException(status_code=404, detail="Prometheus exporter disabled")
    refresh_slo_metrics()
    refresh_queue_backlog_metrics()
    return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


@app.middleware("http")
async def prometheus_http_middleware(request: Request, call_next):
    if not PROMETHEUS_ENABLED:
        return await call_next(request)

    path = request.url.path
    method = request.method
    start = time.perf_counter()
    if TRACER:
        with TRACER.start_as_current_span(f"http {method} {path}"):
            response = await call_next(request)
    else:
        response = await call_next(request)

    elapsed = time.perf_counter() - start
    PROM_HTTP_REQUEST_DURATION_SECONDS.labels(
        service=SERVICE_NAME, env=DEPLOY_ENV, method=method, path=path
    ).observe(elapsed)
    PROM_HTTP_REQUESTS_TOTAL.labels(
        service=SERVICE_NAME, env=DEPLOY_ENV, method=method, path=path, status_code=str(response.status_code)
    ).inc()
    return response


@app.post("/warmup/jobs")
def create_job(payload: WarmupJobRequest) -> dict:
    mailbox = payload.mailbox.lower()
    with Session(engine) as session:
        profile = get_or_create_profile(
            session,
            tenant_id=payload.tenant_id,
            mailbox=mailbox,
            domain_age_days=payload.domain_age_days,
            timezone_name=payload.timezone,
        )

        if is_killed(payload.tenant_id, mailbox):
            profile.mode = "paused"
            session.commit()
            raise HTTPException(status_code=423, detail="Kill switch active for mailbox/tenant/provider")

        base_daily = base_target_from_domain_age(payload.domain_age_days)
        if payload.blacklist_detected:
            base_daily = max(1, int(base_daily * 0.25))
            profile.blacklisted = True
            profile.mode = "quarantine"

        tenant_total = aggregate_tenant_daily_target(session, payload.tenant_id)
        if tenant_total + base_daily > MAX_TENANT_DAILY_CAP:
            raise HTTPException(status_code=429, detail="Tenant warmup cap exceeded")

        interval_minutes = random.randint(8, 55)
        reply_simulation_rate = round(random.uniform(0.18, 0.42), 2)
        spam_rescue_rate = round(random.uniform(0.03, 0.11), 2)

        job_id = f"warmup-{uuid.uuid4().hex[:12]}"
        next_run = utc_now() + timedelta(minutes=interval_minutes)

        profile.current_daily_target = int(clamp(base_daily, 1, MAX_MAILBOX_DAILY_CAP))
        profile.updated_at = utc_now()

        job = WarmupJob(
            job_id=job_id,
            tenant_id=payload.tenant_id,
            mailbox=mailbox,
            status="active" if profile.mode != "paused" else "paused",
            daily_target=profile.current_daily_target,
            interval_minutes=interval_minutes,
            reply_simulation_rate=reply_simulation_rate,
            spam_rescue_rate=spam_rescue_rate,
            next_run_at=next_run,
        )
        session.add(job)
        session.commit()

        METRICS["jobs_created"] += 1
        json_log("warmup_job_created", tenant_id=payload.tenant_id, mailbox=mailbox, job_id=job_id)
        return {
            "job_id": job_id,
            "tenant_id": payload.tenant_id,
            "mailbox": mailbox,
            "daily_target": job.daily_target,
            "interval_minutes": interval_minutes,
            "reply_simulation_rate": reply_simulation_rate,
            "spam_rescue_rate": spam_rescue_rate,
            "blacklist_detected": payload.blacklist_detected,
            "mode": profile.mode,
            "next_run_at": next_run.isoformat(),
        }


@app.get("/warmup/jobs")
def list_jobs(tenant_id: str | None = None, mailbox: str | None = None) -> dict:
    with Session(engine) as session:
        stmt = select(WarmupJob)
        if tenant_id:
            stmt = stmt.where(WarmupJob.tenant_id == tenant_id)
        if mailbox:
            stmt = stmt.where(WarmupJob.mailbox == mailbox.lower())
        rows = list(session.scalars(stmt.order_by(WarmupJob.created_at.desc()).limit(500)))
        return {
            "items": [
                {
                    "job_id": row.job_id,
                    "tenant_id": row.tenant_id,
                    "mailbox": row.mailbox,
                    "status": row.status,
                    "daily_target": row.daily_target,
                    "interval_minutes": row.interval_minutes,
                    "next_run_at": row.next_run_at.isoformat(),
                }
                for row in rows
            ]
        }


@app.post("/warmup/reputation/score")
def update_reputation(payload: ReputationUpdateRequest) -> dict:
    mailbox = payload.mailbox.lower()
    with Session(engine) as session:
        profile = get_or_create_profile(session, payload.tenant_id, mailbox)

        profile.inbox_ewma = ewma(profile.inbox_ewma, payload.inbox_rate)
        profile.spam_ewma = ewma(profile.spam_ewma, payload.spam_rate)
        profile.bounce_ewma = ewma(profile.bounce_ewma, payload.bounce_rate)
        profile.complaint_ewma = ewma(profile.complaint_ewma, payload.complaint_rate)
        profile.reply_ewma = ewma(profile.reply_ewma, payload.reply_rate)
        profile.blacklisted = payload.blacklist_detected

        risk = classify_risk(profile, payload.blacklist_detected)
        profile.risk_score = risk
        profile.reputation_score = clamp(1 - risk, 0.0, 1.0)

        new_target, mode = apply_adaptive_policy(profile, risk, payload.blacklist_detected)
        if is_killed(payload.tenant_id, mailbox):
            mode = "paused"
            new_target = 0

        profile.current_daily_target = int(clamp(new_target, 0, MAX_MAILBOX_DAILY_CAP))
        profile.mode = mode
        profile.updated_at = utc_now()

        session.add(
            RiskSignal(
                tenant_id=payload.tenant_id,
                mailbox=mailbox,
                signal_type="risk_classifier",
                severity=risk,
                details=json.dumps(
                    {
                        "spam_ewma": profile.spam_ewma,
                        "bounce_ewma": profile.bounce_ewma,
                        "complaint_ewma": profile.complaint_ewma,
                        "reply_ewma": profile.reply_ewma,
                        "mode": mode,
                    }
                ),
            )
        )
        session.commit()

        METRICS["reputation_updates"] += 1
        json_log("reputation_updated", tenant_id=payload.tenant_id, mailbox=mailbox, mode=mode, risk=risk)
        return {
            "tenant_id": payload.tenant_id,
            "mailbox": mailbox,
            "ewma": {
                "inbox": round(profile.inbox_ewma, 4),
                "spam": round(profile.spam_ewma, 4),
                "bounce": round(profile.bounce_ewma, 4),
                "complaint": round(profile.complaint_ewma, 4),
                "reply": round(profile.reply_ewma, 4),
            },
            "risk_score": round(risk, 4),
            "reputation_score": round(profile.reputation_score, 4),
            "daily_target": profile.current_daily_target,
            "mode": mode,
        }


@app.post("/warmup/schedule/generate")
def generate_schedule(payload: ScheduleRequest) -> dict:
    mailbox = payload.mailbox.lower()
    with Session(engine) as session:
        profile = get_or_create_profile(session, payload.tenant_id, mailbox)
        if is_killed(payload.tenant_id, mailbox):
            raise HTTPException(status_code=423, detail="Kill switch active")

        count = min(payload.requested_count, max(1, profile.current_daily_target))
        partners, entropy = pick_partners(profile, payload.partner_pool, count)
        schedule = generate_humanized_schedule(profile, partners, count)
        session.commit()

        METRICS["schedules_generated"] += 1
        return {
            "tenant_id": payload.tenant_id,
            "mailbox": mailbox,
            "mode": profile.mode,
            "daily_target": profile.current_daily_target,
            "entropy_score": round(entropy, 4),
            "items": schedule,
        }


@app.post("/warmup/events")
def record_event(payload: QueueTaskRequest) -> dict:
    with Session(engine) as session:
        existing = session.scalar(select(WarmupEvent).where(WarmupEvent.idempotency_key == payload.idempotency_key))
        if existing:
            return {
                "idempotent": True,
                "event_id": existing.id,
                "status": existing.status,
                "retry_count": existing.retry_count,
            }

        event = WarmupEvent(
            tenant_id=payload.tenant_id,
            mailbox=payload.mailbox.lower(),
            event_type="queue_task",
            outcome="accepted",
            queue_name=payload.queue_name,
            idempotency_key=payload.idempotency_key,
            status="queued",
            payload=json.dumps({**payload.payload, "max_attempts": payload.max_attempts}),
        )
        session.add(event)
        session.commit()

        METRICS["events_recorded"] += 1
        return {"idempotent": False, "event_id": event.id, "status": "queued"}


@app.post("/warmup/worker/enqueue")
def enqueue_task(payload: QueueTaskRequest) -> dict:
    with Session(engine) as session:
        existing = session.scalar(select(WarmupEvent).where(WarmupEvent.idempotency_key == payload.idempotency_key))
        if existing:
            return {"idempotent": True, "status": existing.status, "event_id": existing.id}

        event = WarmupEvent(
            tenant_id=payload.tenant_id,
            mailbox=payload.mailbox.lower(),
            event_type="queue_task",
            outcome="accepted",
            queue_name=payload.queue_name,
            idempotency_key=payload.idempotency_key,
            status="queued",
            payload=json.dumps({**payload.payload, "max_attempts": payload.max_attempts}),
        )
        session.add(event)
        session.commit()
        with Session(engine) as outbox_session:
            ensure_outbox_event(
                outbox_session,
                topic="warmup.queue.enqueue",
                dedupe_key=f"warmup-event:{event.id}",
                payload={
                    "event_id": event.id,
                    "tenant_id": payload.tenant_id,
                    "mailbox": payload.mailbox.lower(),
                    "queue_name": payload.queue_name,
                    "idempotency_key": payload.idempotency_key,
                },
            )
            outbox_session.commit()

        queue_payload = {
            "event_id": event.id,
            "queue_name": payload.queue_name,
            "tenant_id": payload.tenant_id,
            "mailbox": payload.mailbox.lower(),
            "idempotency_key": payload.idempotency_key,
            **payload.payload,
            "max_attempts": payload.max_attempts,
            "attempt": 1,
        }

        rq_queue = get_rq_queue(payload.queue_name)
        if rq_queue is not None:
            rq_queue.enqueue(execute_queue_task, queue_payload, job_id=payload.idempotency_key)
            backend = "rq"
        else:
            IN_MEMORY_QUEUES[payload.queue_name].append(queue_payload)
            backend = "in-memory"

        METRICS["queue_enqueued"] += 1
        refresh_queue_backlog_metrics()
        return {"idempotent": False, "backend": backend, "event_id": event.id, "status": "queued"}


@app.post("/warmup/worker/process-next")
def process_next(queue_name: str = "send_execution") -> dict:
    if queue_name not in QUEUE_NAMES:
        raise HTTPException(status_code=400, detail="Invalid queue name")
    sweep_stuck_inflight_tasks()

    rq_queue = get_rq_queue(queue_name)
    if rq_queue is not None:
        if rq_queue.count == 0:
            return {"processed": 0, "backend": "rq"}
        worker = SimpleWorker([rq_queue], connection=rq_queue.connection)
        worker.work(burst=True)
        METRICS["queue_processed"] += 1
        return {"processed": 1, "backend": "rq"}

    if not IN_MEMORY_QUEUES[queue_name]:
        refresh_queue_backlog_metrics()
        return {"processed": 0, "backend": "in-memory"}

    task = IN_MEMORY_QUEUES[queue_name].popleft()
    lease_until = utc_now() + timedelta(seconds=QUEUE_VISIBILITY_TIMEOUT_SECONDS)
    inflight_key = _event_key(queue_name, task.get("event_id"))
    IN_MEMORY_INFLIGHT[inflight_key] = {"task": dict(task), "queue_name": queue_name, "lease_until": lease_until}
    with Session(engine) as session:
        event = session.get(WarmupEvent, task["event_id"])
        if event is None:
            IN_MEMORY_INFLIGHT.pop(inflight_key, None)
            raise HTTPException(status_code=404, detail="Queue event not found")
        lease = session.scalar(select(WorkerLease).where(WorkerLease.event_id == event.id, WorkerLease.queue_name == queue_name))
        if lease:
            lease.lease_until = lease_until
        else:
            session.add(WorkerLease(event_id=event.id, queue_name=queue_name, lease_until=lease_until))
        session.flush()

        try:
            result = execute_queue_task(task)
            event.status = "processed"
            event.outcome = "success"
            mark_outbox_dispatched(session, f"warmup-event:{event.id}")
            session.execute(delete(WorkerLease).where(WorkerLease.event_id == event.id, WorkerLease.queue_name == queue_name))
            session.commit()
            IN_MEMORY_INFLIGHT.pop(inflight_key, None)
            METRICS["queue_processed"] += 1
            refresh_slo_metrics()
            refresh_queue_backlog_metrics()
            return {"processed": 1, "backend": "in-memory", "result": result}
        except Exception as exc:
            IN_MEMORY_INFLIGHT.pop(inflight_key, None)
            event.retry_count += 1
            attempts = task.get("attempt", 1)
            max_attempts = int(task.get("max_attempts", 4))
            if attempts >= max_attempts:
                event.status = "dead_letter"
                event.outcome = "failed"
                DEAD_LETTER_QUEUE.append({"task": task, "error": str(exc), "failed_at": utc_now().isoformat()})
                session.add(
                    RiskSignal(
                        tenant_id=event.tenant_id,
                        mailbox=event.mailbox,
                        signal_type="worker_dead_letter",
                        severity=0.6,
                        details=json.dumps({"error": str(exc), "queue": queue_name}),
                    )
                )
                session.execute(delete(WorkerLease).where(WorkerLease.event_id == event.id, WorkerLease.queue_name == queue_name))
                session.commit()
                METRICS["queue_dead_letter"] += 1
                refresh_slo_metrics()
                refresh_queue_backlog_metrics()
                return {"processed": 0, "backend": "in-memory", "dead_lettered": True}

            event.status = "retrying"
            task["attempt"] = attempts + 1
            task["retry_backoff_seconds"] = 2 ** attempts
            IN_MEMORY_QUEUES[queue_name].append(task)
            session.execute(delete(WorkerLease).where(WorkerLease.event_id == event.id, WorkerLease.queue_name == queue_name))
            session.commit()
            METRICS["queue_retried"] += 1
            refresh_slo_metrics()
            refresh_queue_backlog_metrics()
            return {"processed": 0, "backend": "in-memory", "retry_scheduled": True}


@app.get("/warmup/worker/dlq")
def list_dlq() -> dict:
    return {"items": list(DEAD_LETTER_QUEUE)}


@app.get("/warmup/worker/inflight")
def list_inflight() -> dict:
    items = []
    with Session(engine) as session:
        leases = list(session.scalars(select(WorkerLease).order_by(WorkerLease.lease_until.asc()).limit(500)))
        for lease in leases:
            key = _event_key(lease.queue_name, lease.event_id)
            item = IN_MEMORY_INFLIGHT.get(key, {})
            attempt = (item.get("task") or {}).get("attempt")
            items.append(
                {
                    "key": key,
                    "queue_name": lease.queue_name,
                    "event_id": lease.event_id,
                    "attempt": attempt,
                    "lease_until": lease.lease_until.isoformat(),
                }
            )
    return {"items": items}


@app.post("/warmup/worker/lease/renew")
def renew_lease(payload: LeaseRenewRequest) -> dict:
    with Session(engine) as session:
        lease = session.scalar(
            select(WorkerLease).where(WorkerLease.event_id == payload.event_id, WorkerLease.queue_name == payload.queue_name)
        )
        if not lease:
            raise HTTPException(status_code=404, detail="Inflight lease not found")
        lease.lease_until = utc_now() + timedelta(seconds=payload.extend_seconds)
        session.commit()
    key = _event_key(payload.queue_name, payload.event_id)
    if key in IN_MEMORY_INFLIGHT:
        IN_MEMORY_INFLIGHT[key]["lease_until"] = lease.lease_until
    METRICS["queue_leases_renewed"] += 1
    return {"renewed": True, "event_id": payload.event_id, "queue_name": payload.queue_name}


@app.post("/warmup/worker/sweep-stuck")
def sweep_stuck_tasks(
    x_admin_api_key: str | None = Header(default=None),
    x_admin_actor: str = Header(default="system"),
    authorization: str | None = Header(default=None),
) -> dict:
    claims = require_admin(x_admin_api_key, authorization, permission="warmup:admin")
    actor = claims.get("sub") if claims else x_admin_actor
    result = sweep_stuck_inflight_tasks()
    with Session(engine) as session:
        write_admin_audit_log(
            session,
            actor=actor,
            action="sweep_stuck_tasks",
            resource_type="warmup_queue",
            details=result,
        )
        session.commit()
    return {"swept": True, **result}


@app.get("/warmup/outbox/pending")
def list_outbox_pending(limit: int = 50) -> dict:
    safe_limit = max(1, min(limit, 200))
    with Session(engine) as session:
        rows = list(
            session.scalars(
                select(OutboxEvent).where(OutboxEvent.status == "pending").order_by(OutboxEvent.created_at.asc()).limit(safe_limit)
            )
        )
        return {
            "items": [
                {
                    "id": row.id,
                    "topic": row.topic,
                    "dedupe_key": row.dedupe_key,
                    "status": row.status,
                    "attempts": row.attempts,
                    "created_at": row.created_at.isoformat(),
                }
                for row in rows
            ]
        }


@app.post("/warmup/inbox/record")
def record_inbox_event(payload: InboxRecordRequest) -> dict:
    with Session(engine) as session:
        existing = session.scalar(select(InboxEvent).where(InboxEvent.message_id == payload.message_id))
        if existing:
            return {"idempotent": True, "message_id": payload.message_id}
        session.add(InboxEvent(source=payload.source, message_id=payload.message_id, payload=json.dumps(payload.payload)))
        session.commit()
        return {"idempotent": False, "message_id": payload.message_id}


@app.post("/warmup/worker/dlq/replay")
def replay_dlq_task(
    payload: DlqReplayRequest,
    x_admin_api_key: str | None = Header(default=None),
    x_admin_actor: str = Header(default="system"),
    authorization: str | None = Header(default=None),
) -> dict:
    claims = require_admin(x_admin_api_key, authorization, permission="warmup:admin")
    actor = claims.get("sub") if claims else x_admin_actor
    if payload.item_index >= len(DEAD_LETTER_QUEUE):
        raise HTTPException(status_code=404, detail="DLQ item not found")

    item = DEAD_LETTER_QUEUE[payload.item_index]
    task = dict(item.get("task") or {})
    queue_name = task.get("queue_name", "send_execution")
    if queue_name not in QUEUE_NAMES:
        raise HTTPException(status_code=400, detail="DLQ item has invalid queue")

    task["attempt"] = 1
    task["replayed_at"] = utc_now().isoformat()
    IN_MEMORY_QUEUES[queue_name].append(task)
    DEAD_LETTER_QUEUE.remove(item)
    refresh_queue_backlog_metrics()

    with Session(engine) as session:
        write_admin_audit_log(
            session,
            actor=actor,
            action="dlq_replay",
            resource_type="warmup_event",
            resource_id=str(task.get("event_id", "")),
            details={"queue_name": queue_name, "reason": payload.reason, "approved_by": payload.approved_by},
        )
        session.commit()

    METRICS["queue_replayed"] += 1
    return {"replayed": True, "queue_name": queue_name, "event_id": task.get("event_id")}


@app.post("/warmup/deliverability/check")
def deliverability_check(payload: DeliverabilityCheckRequest) -> dict:
    domain = payload.domain.lower()
    mailbox = payload.mailbox.lower()

    spf_aligned = "v=spf1" in maybe_resolve_dns_txt(domain)
    dmarc_aligned = "v=DMARC1" in maybe_resolve_dns_txt(f"_dmarc.{domain}")
    dkim_txt = maybe_resolve_dns_txt(f"{payload.dkim_selector}._domainkey.{domain}")
    dkim_aligned = "k=" in dkim_txt or "v=DKIM1" in dkim_txt

    ptr_valid = payload.ptr_valid if payload.ptr_valid is not None else False
    tls_supported = payload.tls_supported if payload.tls_supported is not None else True

    score = (
        (0.18 if spf_aligned else 0.0)
        + (0.2 if dkim_aligned else 0.0)
        + (0.2 if dmarc_aligned else 0.0)
        + (0.12 if ptr_valid else 0.0)
        + (0.1 if tls_supported else 0.0)
        + payload.inbox_pct * 0.2
        + (1 - payload.spam_pct) * 0.08
    )
    score = round(clamp(score, 0.0, 1.0), 4)

    placement = "inbox"
    if payload.spam_pct >= 0.2:
        placement = "spam"
        METRICS["deliverability_spam_events"] += 1
    elif payload.promotions_pct >= payload.inbox_pct:
        placement = "promotions"

    with Session(engine) as session:
        session.add(
            DeliverabilitySnapshot(
                tenant_id=payload.tenant_id,
                mailbox=mailbox,
                domain=domain,
                spf_aligned=spf_aligned,
                dkim_aligned=dkim_aligned,
                dmarc_aligned=dmarc_aligned,
                ptr_valid=ptr_valid,
                tls_supported=tls_supported,
                inbox_pct=payload.inbox_pct,
                promotions_pct=payload.promotions_pct,
                spam_pct=payload.spam_pct,
                score=score,
            )
        )

        session.add(
            RiskSignal(
                tenant_id=payload.tenant_id,
                mailbox=mailbox,
                signal_type="deliverability_check",
                severity=round(1 - score, 4),
                details=json.dumps({"domain": domain, "placement": placement}),
            )
        )
        session.commit()

    METRICS["deliverability_checks"] += 1
    return {
        "tenant_id": payload.tenant_id,
        "mailbox": mailbox,
        "domain": domain,
        "spf_aligned": spf_aligned,
        "dkim_aligned": dkim_aligned,
        "dmarc_aligned": dmarc_aligned,
        "ptr_valid": ptr_valid,
        "tls_supported": tls_supported,
        "placement_category": placement,
        "deliverability_score": score,
    }


@app.post("/warmup/content/plan")
def content_plan(payload: ContentPlanRequest) -> dict:
    day = payload.day_number
    if day <= 7:
        stage = "foundation"
        allow_links = False
        allow_images = False
        lexical_diversity = 0.25
    elif day <= 14:
        stage = "expansion"
        allow_links = False
        allow_images = day > 10
        lexical_diversity = 0.35
    elif day <= 30:
        stage = "trust-building"
        allow_links = True
        allow_images = True
        lexical_diversity = 0.45
    else:
        stage = "steady-state"
        allow_links = True
        allow_images = True
        lexical_diversity = 0.55

    variant_seed = f"{payload.tenant_id}:{payload.mailbox.lower()}:{day}"
    signature_variant = int(hash(variant_seed) % 5) + 1

    METRICS["content_plans"] += 1
    return {
        "tenant_id": payload.tenant_id,
        "mailbox": payload.mailbox.lower(),
        "day_number": day,
        "stage": stage,
        "allow_links": allow_links,
        "allow_images": allow_images,
        "lexical_diversity": lexical_diversity,
        "signature_variant": signature_variant,
        "template_policy": {
            "semantic_preservation": True,
            "bounded_mutation": True,
            "max_template_similarity": 0.86,
        },
    }


@app.post("/warmup/abuse/check")
def abuse_check(payload: AbuseCheckRequest) -> dict:
    mailbox = payload.mailbox.lower()
    severity = 0.0
    signals: list[str] = []

    if payload.complaint_spike:
        severity += 0.4
        signals.append("complaint_spike")
    if payload.repeated_bad_domains >= 5:
        severity += 0.3
        signals.append("repeated_bad_domains")
    if payload.anomalous_burstiness >= 0.7:
        severity += 0.35
        signals.append("anomalous_burstiness")

    severity = clamp(severity, 0.0, 1.0)
    blocked = severity >= 0.65

    with Session(engine) as session:
        session.add(
            RiskSignal(
                tenant_id=payload.tenant_id,
                mailbox=mailbox,
                signal_type="abuse_heuristics",
                severity=severity,
                details=json.dumps({"signals": signals}),
            )
        )

        profile = get_or_create_profile(session, payload.tenant_id, mailbox)
        if blocked:
            profile.mode = "paused"
            profile.current_daily_target = 0
        session.commit()

    METRICS["abuse_checks"] += 1
    return {
        "tenant_id": payload.tenant_id,
        "mailbox": mailbox,
        "severity": round(severity, 4),
        "signals": signals,
        "blocked": blocked,
    }


@app.post("/warmup/kill-switch")
def set_kill_switch(
    payload: KillSwitchRequest,
    x_admin_api_key: str | None = Header(default=None),
    x_admin_actor: str = Header(default="system"),
    authorization: str | None = Header(default=None),
) -> dict:
    global GLOBAL_KILL_SWITCH
    tenant_scope = payload.value if payload.scope == "tenant" else None
    claims = require_admin(x_admin_api_key, authorization, permission="warmup:admin", tenant_scope=tenant_scope)
    actor = claims.get("sub") if claims else x_admin_actor

    if payload.scope == "global":
        GLOBAL_KILL_SWITCH = payload.enabled
    elif payload.scope == "tenant":
        if not payload.value:
            raise HTTPException(status_code=400, detail="tenant scope requires value")
        if payload.enabled:
            TENANT_KILL_SWITCHES.add(payload.value)
        else:
            TENANT_KILL_SWITCHES.discard(payload.value)
    elif payload.scope == "provider":
        if not payload.value:
            raise HTTPException(status_code=400, detail="provider scope requires value")
        provider = payload.value.lower()
        if payload.enabled:
            PROVIDER_KILL_SWITCHES.add(provider)
        else:
            PROVIDER_KILL_SWITCHES.discard(provider)

    with Session(engine) as session:
        write_admin_audit_log(
            session,
            actor=actor,
            action="kill_switch_update",
            resource_type="kill_switch",
            resource_id=payload.scope,
            details={"enabled": payload.enabled, "value": payload.value},
        )
        session.commit()

    METRICS["kill_switch_updates"] += 1
    return {
        "global": GLOBAL_KILL_SWITCH,
        "tenants": sorted(TENANT_KILL_SWITCHES),
        "providers": sorted(PROVIDER_KILL_SWITCHES),
    }


@app.post("/warmup/admin/internal-mailboxes")
def upsert_internal_mailbox(
    payload: InternalMailboxRequest,
    x_admin_api_key: str | None = Header(default=None),
    x_admin_actor: str = Header(default="system"),
    authorization: str | None = Header(default=None),
) -> dict:
    claims = require_admin(x_admin_api_key, authorization, permission="warmup:admin", tenant_scope=payload.tenant_id)
    actor = claims.get("sub") if claims else x_admin_actor
    mailbox = payload.mailbox.lower()
    provider = provider_from_mailbox(mailbox)
    with Session(engine) as session:
        item = session.scalar(
            select(InternalMailbox).where(
                InternalMailbox.tenant_id == payload.tenant_id, InternalMailbox.mailbox == mailbox
            )
        )
        created = False
        if item is None:
            item = InternalMailbox(tenant_id=payload.tenant_id, mailbox=mailbox, provider=provider, notes=payload.notes)
            session.add(item)
            created = True
        else:
            item.provider = provider
            item.notes = payload.notes
            item.is_active = True
        write_admin_audit_log(
            session,
            actor=actor,
            action="internal_mailbox_upsert",
            resource_type="internal_mailbox",
            resource_id=f"{payload.tenant_id}:{mailbox}",
            details={"provider": provider, "created": created},
        )
        session.commit()

    return {
        "created": created,
        "tenant_id": payload.tenant_id,
        "mailbox": mailbox,
        "provider": provider,
        "notes": payload.notes,
        "is_active": True,
    }


@app.get("/warmup/admin/internal-mailboxes")
def list_internal_mailboxes(tenant_id: str | None = None) -> dict:
    with Session(engine) as session:
        stmt = select(InternalMailbox)
        if tenant_id:
            stmt = stmt.where(InternalMailbox.tenant_id == tenant_id)
        records = session.scalars(stmt.order_by(InternalMailbox.tenant_id, InternalMailbox.mailbox)).all()
        return {
            "items": [
                {
                    "tenant_id": record.tenant_id,
                    "mailbox": record.mailbox,
                    "provider": record.provider,
                    "is_active": record.is_active,
                    "notes": record.notes,
                }
                for record in records
            ]
        }


@app.get("/warmup/admin/mailbox-health")
def mailbox_health(tenant_id: str, mailbox: str, limit: int = 20) -> dict:
    mailbox_normalized = mailbox.lower()
    capped_limit = max(1, min(limit, 100))
    with Session(engine) as session:
        profile = session.scalar(
            select(MailboxProfile).where(MailboxProfile.tenant_id == tenant_id, MailboxProfile.mailbox == mailbox_normalized)
        )
        snapshots = session.scalars(
            select(DeliverabilitySnapshot)
            .where(DeliverabilitySnapshot.tenant_id == tenant_id, DeliverabilitySnapshot.mailbox == mailbox_normalized)
            .order_by(DeliverabilitySnapshot.created_at.desc())
            .limit(capped_limit)
        ).all()
        events = session.scalars(
            select(WarmupEvent)
            .where(WarmupEvent.tenant_id == tenant_id, WarmupEvent.mailbox == mailbox_normalized)
            .order_by(WarmupEvent.created_at.desc())
            .limit(capped_limit)
        ).all()

    return {
        "tenant_id": tenant_id,
        "mailbox": mailbox_normalized,
        "profile": (
            {
                "mode": profile.mode,
                "daily_target": profile.current_daily_target,
                "risk_score": round(profile.risk_score, 4),
                "reputation_score": round(profile.reputation_score, 4),
            }
            if profile
            else None
        ),
        "deliverability_timeline": [
            {
                "created_at": snapshot.created_at.isoformat(),
                "score": snapshot.score,
                "inbox_pct": snapshot.inbox_pct,
                "spam_pct": snapshot.spam_pct,
            }
            for snapshot in snapshots
        ],
        "event_timeline": [
            {
                "id": event.id,
                "status": event.status,
                "queue_name": event.queue_name,
                "retry_count": event.retry_count,
                "created_at": event.created_at.isoformat(),
            }
            for event in events
        ],
    }


@app.get("/warmup/admin/audit-logs")
def list_admin_audit_logs(limit: int = 100) -> dict:
    capped_limit = max(1, min(limit, 300))
    with Session(engine) as session:
        rows = session.scalars(select(AdminAuditLog).order_by(AdminAuditLog.created_at.desc()).limit(capped_limit)).all()
        return {
            "items": [
                {
                    "actor": row.actor,
                    "action": row.action,
                    "resource_type": row.resource_type,
                    "resource_id": row.resource_id,
                    "details": json.loads(row.details or "{}"),
                    "created_at": row.created_at.isoformat(),
                }
                for row in rows
            ]
        }
