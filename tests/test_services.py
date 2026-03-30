import importlib.util
import sys
import uuid
from pathlib import Path

from fastapi.testclient import TestClient


def load_app(module_path: str):
    module_name = f"service_module_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module.app


ROOT = Path(__file__).resolve().parents[1]
auth_app = load_app(str(ROOT / "backend/services/auth/app/main.py"))
warmup_app = load_app(str(ROOT / "warmup-engine/app/main.py"))
verification_app = load_app(str(ROOT / "verification-engine/app/main.py"))


def test_auth_signup_login_and_verify():
    client = TestClient(auth_app)

    signup = client.post(
        "/signup",
        json={
            "email": "owner@example.com",
            "password": "StrongPass123",
            "role": "client",
            "tenant_id": "tenant-a",
        },
    )
    assert signup.status_code == 200
    token = signup.json()["access_token"]

    login = client.post("/login", json={"email": "owner@example.com", "password": "StrongPass123"})
    assert login.status_code == 200

    verify = client.post("/verify-token", json={"token": token})
    assert verify.status_code == 200
    assert verify.json()["tenant_id"] == "tenant-a"


def test_warmup_job_creation():
    client = TestClient(warmup_app)
    response = client.post(
        "/warmup/jobs",
        json={
            "tenant_id": "tenant-a",
            "mailbox": "warmup@example.com",
            "domain_age_days": 30,
            "blacklist_detected": False,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["job_id"].startswith("warmup-")
    assert body["daily_target"] >= 5


def test_verification_syntax_check_rejects_invalid_email():
    client = TestClient(verification_app)
    response = client.post("/verify", json={"email": "not-an-email"})
    assert response.status_code == 422


def test_warmup_reputation_scoring_and_mode_changes():
    client = TestClient(warmup_app)

    created = client.post(
        "/warmup/jobs",
        json={
            "tenant_id": "tenant-risk",
            "mailbox": "risk@example.com",
            "domain_age_days": 10,
            "blacklist_detected": False,
            "timezone": "UTC",
        },
    )
    assert created.status_code == 200

    high_risk = client.post(
        "/warmup/reputation/score",
        json={
            "tenant_id": "tenant-risk",
            "mailbox": "risk@example.com",
            "inbox_rate": 0.2,
            "spam_rate": 0.7,
            "bounce_rate": 0.4,
            "complaint_rate": 0.2,
            "reply_rate": 0.01,
            "blacklist_detected": True,
        },
    )
    assert high_risk.status_code == 200
    assert high_risk.json()["mode"] in {"quarantine", "throttle"}


def test_warmup_schedule_generation_and_entropy():
    client = TestClient(warmup_app)

    client.post(
        "/warmup/jobs",
        json={
            "tenant_id": "tenant-schedule",
            "mailbox": "scheduler@example.com",
            "domain_age_days": 45,
            "blacklist_detected": False,
            "timezone": "UTC",
        },
    )

    schedule = client.post(
        "/warmup/schedule/generate",
        json={
            "tenant_id": "tenant-schedule",
            "mailbox": "scheduler@example.com",
            "partner_pool": [
                "a@partners.test",
                "b@partners.test",
                "c@partners.test",
                "d@partners.test",
            ],
            "requested_count": 12,
        },
    )
    assert schedule.status_code == 200
    body = schedule.json()
    assert body["entropy_score"] > 0
    assert len(body["items"]) >= 1
    assert all("send_at" in item for item in body["items"])


def test_warmup_queue_idempotency_and_dlq_flow():
    client = TestClient(warmup_app)

    idem = f"tenant-queue-mailbox-{uuid.uuid4().hex[:16]}"
    first = client.post(
        "/warmup/worker/enqueue",
        json={
            "tenant_id": "tenant-queue",
            "mailbox": "queue@example.com",
            "queue_name": "send_execution",
            "idempotency_key": idem,
            "payload": {"force_fail": True},
            "max_attempts": 1,
        },
    )
    assert first.status_code == 200
    assert first.json()["idempotent"] is False

    second = client.post(
        "/warmup/worker/enqueue",
        json={
            "tenant_id": "tenant-queue",
            "mailbox": "queue@example.com",
            "queue_name": "send_execution",
            "idempotency_key": idem,
            "payload": {"force_fail": True},
            "max_attempts": 1,
        },
    )
    assert second.status_code == 200
    assert second.json()["idempotent"] is True

    processed = client.post("/warmup/worker/process-next", params={"queue_name": "send_execution"})
    assert processed.status_code == 200

    dlq = client.get("/warmup/worker/dlq")
    assert dlq.status_code == 200
    assert len(dlq.json()["items"]) >= 1


def test_warmup_deliverability_and_content_policy():
    client = TestClient(warmup_app)

    check = client.post(
        "/warmup/deliverability/check",
        json={
            "tenant_id": "tenant-deliver",
            "mailbox": "deliver@example.com",
            "domain": "example.com",
            "ptr_valid": True,
            "tls_supported": True,
            "inbox_pct": 0.6,
            "promotions_pct": 0.2,
            "spam_pct": 0.2,
        },
    )
    assert check.status_code == 200
    assert 0 <= check.json()["deliverability_score"] <= 1

    plan = client.post(
        "/warmup/content/plan",
        json={
            "tenant_id": "tenant-deliver",
            "mailbox": "deliver@example.com",
            "day_number": 3,
        },
    )
    assert plan.status_code == 200
    assert plan.json()["allow_links"] is False


def test_warmup_abuse_and_kill_switch():
    client = TestClient(warmup_app)

    abuse = client.post(
        "/warmup/abuse/check",
        json={
            "tenant_id": "tenant-abuse",
            "mailbox": "abuse@example.com",
            "complaint_spike": True,
            "repeated_bad_domains": 10,
            "anomalous_burstiness": 0.9,
        },
    )
    assert abuse.status_code == 200
    assert abuse.json()["blocked"] is True

    kill = client.post(
        "/warmup/kill-switch",
        json={"scope": "tenant", "enabled": True, "value": "tenant-killed"},
    )
    assert kill.status_code == 200

    blocked_job = client.post(
        "/warmup/jobs",
        json={
            "tenant_id": "tenant-killed",
            "mailbox": "stop@example.com",
            "domain_age_days": 10,
            "blacklist_detected": False,
            "timezone": "UTC",
        },
    )
    assert blocked_job.status_code == 423
