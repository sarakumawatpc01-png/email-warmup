from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="AI Agent", version="1.0.0")


class AgentInput(BaseModel):
    tenant_id: str
    campaigns: list[dict] = []
    crm: list[dict] = []
    engagement: dict = {}


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "ai-agent"}


@app.post("/run")
def run(payload: AgentInput) -> dict:
    actions = []
    if not payload.campaigns:
        actions.append(
            {
                "action": "create_campaign",
                "channel": "email",
                "reason": "No active campaigns found",
            }
        )

    open_crm_items = [item for item in payload.crm if item.get("status") in {"new", "open"}]
    if open_crm_items:
        actions.append(
            {
                "action": "send_follow_up",
                "count": len(open_crm_items),
                "reason": "Open CRM items require engagement",
            }
        )

    if payload.engagement.get("bounce_rate", 0) > 0.05:
        actions.append(
            {
                "action": "update_crm",
                "field": "risk",
                "value": "high_bounce",
            }
        )

    return {
        "tenant_id": payload.tenant_id,
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "next_run_minutes": 30,
        "actions": actions,
    }
