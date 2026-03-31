import os
import time
import uuid
import hmac
import hashlib
import logging
from collections import defaultdict, deque
from threading import Lock

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

logger = logging.getLogger("backend-gateway")

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
except ImportError as exc:  # pragma: no cover - optional runtime capability
    logger.warning("OpenTelemetry unavailable; tracing disabled: %s", exc)
    trace = None
    OTLPSpanExporter = None
    FastAPIInstrumentor = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    ConsoleSpanExporter = None

app = FastAPI(title="API Gateway", version="1.0.0")
# localhost defaults: 80=nginx entrypoint, 5173=client app, 5174=superadmin app
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "http://localhost:80,http://localhost:5173,http://localhost:5174").split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

TARGETS = {
    "auth": os.getenv("AUTH_SERVICE_URL", "http://auth:8001"),
    "leads": os.getenv("LEAD_SERVICE_URL", "http://lead_service:8002"),
    "warmup": os.getenv("WARMUP_SERVICE_URL", "http://warmup_engine:8003"),
    "verification": os.getenv("VERIFICATION_SERVICE_URL", "http://verification_engine:8004"),
    "ai": os.getenv("AI_AGENT_SERVICE_URL", "http://ai_agent:8005"),
    "billing": os.getenv("BILLING_SERVICE_URL", "http://billing_service:3001"),
    "whatsapp": os.getenv("WHATSAPP_SERVICE_URL", "http://whatsapp_service:3002"),
    "crm": os.getenv("CRM_SERVICE_URL", "http://crm:8070"),
}
SERVICE_IDENTITIES = {
    "auth": "spiffe://email-warmup/auth",
    "leads": "spiffe://email-warmup/lead-service",
    "warmup": "spiffe://email-warmup/warmup-engine",
    "verification": "spiffe://email-warmup/verification-engine",
    "ai": "spiffe://email-warmup/ai-agent",
    "billing": "spiffe://email-warmup/billing-service",
    "whatsapp": "spiffe://email-warmup/whatsapp-service",
    "crm": "spiffe://email-warmup/crm",
}

OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
OTEL_ENABLE_CONSOLE_EXPORTER = os.getenv("OTEL_ENABLE_CONSOLE_EXPORTER", "false").lower() == "true"
GATEWAY_SIGNING_SECRET = os.getenv("GATEWAY_SIGNING_SECRET", "dev-gateway-signing-secret")
DEPLOY_ENV = os.getenv("DEPLOY_ENV", "dev").lower()
GATEWAY_MAX_REQUEST_BYTES = int(os.getenv("GATEWAY_MAX_REQUEST_BYTES", str(10 * 1024 * 1024)))
SECRETS_MANAGER_URL = os.getenv("SECRETS_MANAGER_URL", "").rstrip("/")
SECRETS_MANAGER_TOKEN = os.getenv("SECRETS_MANAGER_TOKEN", "")
GATEWAY_ADMIN_RATE_LIMIT = int(os.getenv("GATEWAY_ADMIN_RATE_LIMIT", "30"))
GATEWAY_ADMIN_RATE_WINDOW_SECONDS = int(os.getenv("GATEWAY_ADMIN_RATE_WINDOW_SECONDS", "60"))
_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = defaultdict(deque)
_RATE_LIMIT_LOCK = Lock()


def _is_non_production_env() -> bool:
    return DEPLOY_ENV in {"dev", "test", "local"}


def _resolve_secret(env_name: str, default: str) -> str:
    value = os.getenv(env_name)
    if value and not value.startswith("sm://"):
        return value
    secret_name = value.removeprefix("sm://") if value else env_name
    if not SECRETS_MANAGER_URL:
        return default
    try:
        headers = {}
        if SECRETS_MANAGER_TOKEN:
            headers["authorization"] = f"Bearer {SECRETS_MANAGER_TOKEN}"
        with httpx.Client(timeout=3) as client:
            response = client.get(f"{SECRETS_MANAGER_URL}/v1/secrets/{secret_name}", headers=headers)
        if response.status_code != 200:
            return default
        payload = response.json()
        resolved = payload.get("value")
        return resolved if isinstance(resolved, str) and resolved else default
    except Exception:
        logger.warning("secret resolution failed for %s", env_name)
        return default


def _rate_limit_key(request: Request) -> str:
    request_id = request.headers.get("x-admin-actor") or request.headers.get("x-caller-service")
    if request_id:
        return request_id.strip().lower()
    return (request.client.host if request.client else "unknown").strip().lower()


def _is_rate_limit_allowed(key: str, limit: int, window_seconds: int) -> bool:
    now = time.monotonic()
    with _RATE_LIMIT_LOCK:
        bucket = _RATE_LIMIT_BUCKETS[key]
        while bucket and now - bucket[0] > window_seconds:
            bucket.popleft()
        if len(bucket) >= limit:
            return False
        bucket.append(now)
        return True


def _enforce_runtime_secrets() -> None:
    if _is_non_production_env():
        return
    if GATEWAY_SIGNING_SECRET == "dev-gateway-signing-secret":
        raise RuntimeError("GATEWAY_SIGNING_SECRET must be set in non-dev environments")

if trace and TracerProvider and BatchSpanProcessor:
    resource = Resource.create({"service.name": "backend-gateway"}) if Resource else None
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

TRACER = trace.get_tracer("backend-gateway") if trace else None


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "gateway"}


async def proxy(request: Request, service: str, path: str) -> Response:
    base = TARGETS[service]
    url = f"{base}/{path}" if path else base
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    request_id = getattr(request.state, "request_id", request.headers.get("x-request-id", str(uuid.uuid4())))
    headers["x-request-id"] = request_id
    headers["x-correlation-id"] = request_id
    headers["x-caller-service"] = "gateway"
    headers["x-caller-identity"] = "spiffe://email-warmup/gateway"
    headers["x-target-service"] = service
    headers["x-target-identity"] = SERVICE_IDENTITIES.get(service, "spiffe://email-warmup/unknown")
    if request.headers.get("traceparent"):
        headers["traceparent"] = request.headers["traceparent"]
    if request.headers.get("tracestate"):
        headers["tracestate"] = request.headers["tracestate"]
    body = await request.body()
    if len(body) > GATEWAY_MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="Request body too large")
    signed_at = str(int(time.time()))
    canonical = "|".join(
        [
            headers["x-caller-service"],
            headers["x-caller-identity"],
            headers["x-target-service"],
            request_id,
            signed_at,
        ]
    )
    signature = hmac.new(
        GATEWAY_SIGNING_SECRET.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers["x-gateway-signed-at"] = signed_at
    headers["x-gateway-signature"] = signature

    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.request(
            method=request.method,
            url=url,
            headers=headers,
            params=dict(request.query_params),
            content=body,
        )

    if response.status_code >= 400:
        detail = (getattr(response, "text", "") or "").strip()
        if len(detail) > 500:
            detail = f"{detail[:500]}..."
        raise HTTPException(
            status_code=response.status_code,
            detail=detail or "Upstream service request failed",
        )
    forwarded_headers = {}
    for key in ("content-type", "location", "cache-control", "etag"):
        value = response.headers.get(key)
        if value:
            forwarded_headers[key] = value
    response_body = getattr(response, "content", None)
    if response_body is None:
        response_body = str(getattr(response, "text", "")).encode("utf-8")
    return Response(content=response_body, status_code=response.status_code, headers=forwarded_headers)


@app.middleware("http")
async def otel_gateway_middleware(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > GATEWAY_MAX_REQUEST_BYTES:
                return JSONResponse(status_code=413, content={"detail": "Request body too large"})
        except ValueError:
            return JSONResponse(status_code=400, content={"detail": "Invalid Content-Length"})
    if request.url.path.startswith("/policy/"):
        key = f"policy:{_rate_limit_key(request)}"
        if not _is_rate_limit_allowed(key, GATEWAY_ADMIN_RATE_LIMIT, GATEWAY_ADMIN_RATE_WINDOW_SECONDS):
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})
    start = time.perf_counter()
    request.state.request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    if TRACER:
        with TRACER.start_as_current_span(f"gateway {request.method} {request.url.path}") as span:
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.route", request.url.path)
            span.set_attribute("service.identity", "spiffe://email-warmup/gateway")
            response = await call_next(request)
            span.set_attribute("http.status_code", response.status_code)
    else:
        response = await call_next(request)
    response.headers["x-request-id"] = request.state.request_id
    response.headers["x-correlation-id"] = request.state.request_id
    response.headers["x-gateway-latency-ms"] = str(round((time.perf_counter() - start) * 1000, 2))
    return response


@app.post("/policy/authorize")
async def policy_authorize(request: Request) -> dict:
    body = await request.json()
    auth_url = f"{TARGETS['auth']}/authorize"
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(auth_url, json=body)
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


@app.post("/policy/consensus")
async def policy_consensus(request: Request) -> dict:
    caller_service = request.headers.get("x-caller-service", "").strip().lower()
    if caller_service not in TARGETS:
        raise HTTPException(status_code=403, detail="Forbidden")
    return {"consensus_decision": "adopt", "confidence": 0.0, "sources": []}


@app.api_route("/auth/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def auth_proxy(request: Request, path: str = ""):
    return await proxy(request, "auth", path)


@app.api_route("/leads/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def lead_proxy(request: Request, path: str = ""):
    return await proxy(request, "leads", path)


@app.api_route("/warmup/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def warmup_proxy(request: Request, path: str = ""):
    return await proxy(request, "warmup", path)


@app.api_route("/verification/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def verification_proxy(request: Request, path: str = ""):
    return await proxy(request, "verification", path)


@app.api_route("/ai/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def ai_proxy(request: Request, path: str = ""):
    return await proxy(request, "ai", path)


@app.api_route("/billing/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def billing_proxy(request: Request, path: str = ""):
    return await proxy(request, "billing", path)


@app.api_route("/whatsapp/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def whatsapp_proxy(request: Request, path: str = ""):
    return await proxy(request, "whatsapp", path)


@app.api_route("/crm/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def crm_proxy(request: Request, path: str = ""):
    return await proxy(request, "crm", path)


GATEWAY_SIGNING_SECRET = _resolve_secret("GATEWAY_SIGNING_SECRET", GATEWAY_SIGNING_SECRET)
_enforce_runtime_secrets()
