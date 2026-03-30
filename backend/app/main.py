import os

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

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


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "gateway"}


async def proxy(request: Request, service: str, path: str) -> dict | str:
    base = TARGETS[service]
    url = f"{base}/{path}" if path else base
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
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
