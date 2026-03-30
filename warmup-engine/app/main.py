import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict

from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(title="Warmup Engine", version="1.0.0")

jobs: Dict[str, dict] = {}


class WarmupJobRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=64)
    mailbox: str
    domain_age_days: int = Field(ge=0)
    blacklist_detected: bool = False


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "warmup-engine"}


@app.post("/warmup/jobs")
def create_job(payload: WarmupJobRequest) -> dict:
    ramp_factor = min(1.0, max(0.1, payload.domain_age_days / 120))
    base_daily = int(5 + 95 * ramp_factor)
    if payload.blacklist_detected:
        base_daily = max(1, int(base_daily * 0.25))

    interval_minutes = random.randint(8, 55)
    reply_simulation_rate = round(random.uniform(0.18, 0.42), 2)
    spam_rescue_rate = round(random.uniform(0.03, 0.11), 2)

    job_id = f"warmup-{uuid.uuid4().hex[:12]}"
    next_run = datetime.now(timezone.utc) + timedelta(minutes=interval_minutes)
    jobs[job_id] = {
        "job_id": job_id,
        "tenant_id": payload.tenant_id,
        "mailbox": payload.mailbox,
        "daily_target": base_daily,
        "interval_minutes": interval_minutes,
        "reply_simulation_rate": reply_simulation_rate,
        "spam_rescue_rate": spam_rescue_rate,
        "blacklist_detected": payload.blacklist_detected,
        "next_run_at": next_run.isoformat(),
    }
    return jobs[job_id]


@app.get("/warmup/jobs")
def list_jobs() -> dict:
    return {"items": list(jobs.values())}
