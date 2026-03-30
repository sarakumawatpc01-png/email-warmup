import os
import time
import uuid

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

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

OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
OTEL_ENABLE_CONSOLE_EXPORTER = os.getenv("OTEL_ENABLE_CONSOLE_EXPORTER", "false").lower() == "true"

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


async def proxy(request: Request, service: str, path: str) -> dict | str:
    base = TARGETS[service]
    url = f"{base}/{path}" if path else base
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    request_id = getattr(request.state, "request_id", request.headers.get("x-request-id", str(uuid.uuid4())))
    headers["x-request-id"] = request_id
    if request.headers.get("traceparent"):
        headers["traceparent"] = request.headers["traceparent"]
    if request.headers.get("tracestate"):
        headers["tracestate"] = request.headers["tracestate"]
    body = await request.body()

    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.request(
            method=request.method,
            url=url,
            headers=headers,
            params=dict(request.query_params),
            content=body,
        )

    content_type = response.headers.get("content-type", "")
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    if "application/json" in content_type:
        return response.json()
    return response.text


@app.middleware("http")
async def otel_gateway_middleware(request: Request, call_next):
    start = time.perf_counter()
    request.state.request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    if TRACER:
        with TRACER.start_as_current_span(f"gateway {request.method} {request.url.path}") as span:
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.route", request.url.path)
            response = await call_next(request)
            span.set_attribute("http.status_code", response.status_code)
    else:
        response = await call_next(request)
    response.headers["x-request-id"] = request.state.request_id
    response.headers["x-gateway-latency-ms"] = str(round((time.perf_counter() - start) * 1000, 2))
    return response


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
