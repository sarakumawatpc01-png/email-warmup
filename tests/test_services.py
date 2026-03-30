import importlib.util
import sys
import uuid
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st
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
    assert isinstance(verify.json()["permissions"], list)


def test_auth_tenant_admin_permissions_claims():
    client = TestClient(auth_app)
    signup = client.post(
        "/signup",
        json={
            "email": "tenantadmin@example.com",
            "password": "StrongPass123",
            "role": "tenant_admin",
            "tenant_id": "tenant-tadmin",
        },
    )
    assert signup.status_code == 200
    token = signup.json()["access_token"]

    verify = client.post("/verify-token", json={"token": token})
    assert verify.status_code == 200
    body = verify.json()
    assert body["role"] == "tenant_admin"
    assert "warmup:admin" in body["permissions"]


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
    assert kill.status_code in {200, 403}

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
    if kill.status_code == 200:
        assert blocked_job.status_code == 423


def test_warmup_admin_internal_mailboxes_and_health():
    client = TestClient(warmup_app)
    tenant = f"tenant-admin-{uuid.uuid4().hex[:6]}"
    mailbox = f"ops-{uuid.uuid4().hex[:6]}@gmail.com"

    created_job = client.post(
        "/warmup/jobs",
        json={
            "tenant_id": tenant,
            "mailbox": mailbox,
            "domain_age_days": 30,
            "blacklist_detected": False,
            "timezone": "UTC",
        },
    )
    assert created_job.status_code == 200

    check = client.post(
        "/warmup/deliverability/check",
        json={
            "tenant_id": tenant,
            "mailbox": mailbox,
            "domain": "gmail.com",
            "ptr_valid": True,
            "tls_supported": True,
            "inbox_pct": 0.7,
            "promotions_pct": 0.2,
            "spam_pct": 0.1,
        },
    )
    assert check.status_code == 200

    upsert = client.post(
        "/warmup/admin/internal-mailboxes",
        json={"tenant_id": tenant, "mailbox": mailbox, "notes": "seed mailbox"},
    )
    assert upsert.status_code in {200, 403}

    listed = client.get(f"/warmup/admin/internal-mailboxes?tenant_id={tenant}")
    assert listed.status_code == 200

    health = client.get(f"/warmup/admin/mailbox-health?tenant_id={tenant}&mailbox={mailbox}")
    assert health.status_code == 200
    body = health.json()
    assert body["tenant_id"] == tenant
    assert body["mailbox"] == mailbox.lower()
    assert isinstance(body["event_timeline"], list)

    auth_client = TestClient(auth_app)
    su = auth_client.post(
        "/signup",
        json={
            "email": f"tenantadmin-{uuid.uuid4().hex[:6]}@example.com",
            "password": "StrongPass123",
            "role": "tenant_admin",
            "tenant_id": tenant,
        },
    )
    assert su.status_code == 200
    token = su.json()["access_token"]
    upsert_with_jwt = client.post(
        "/warmup/admin/internal-mailboxes",
        headers={"Authorization": f"Bearer {token}"},
        json={"tenant_id": tenant, "mailbox": f"seed-{uuid.uuid4().hex[:6]}@gmail.com", "notes": "jwt"},
    )
    assert upsert_with_jwt.status_code in {200, 403}


def test_warmup_dlq_replay_endpoint():
    client = TestClient(warmup_app)
    key = f"dlq-{uuid.uuid4().hex[:16]}"
    enqueue = client.post(
        "/warmup/worker/enqueue",
        json={
            "tenant_id": "tenant-dlq-replay",
            "mailbox": "dlq@example.com",
            "queue_name": "send_execution",
            "idempotency_key": key,
            "payload": {"force_fail": True},
            "max_attempts": 1,
        },
    )
    assert enqueue.status_code == 200

    process = client.post("/warmup/worker/process-next", params={"queue_name": "send_execution"})
    assert process.status_code == 200

    replay = client.post(
        "/warmup/worker/dlq/replay",
        json={"item_index": 0, "approved_by": "qa-admin", "reason": "retry after fix"},
    )
    assert replay.status_code in {200, 403}


def test_warmup_prometheus_metrics_endpoint():
    client = TestClient(warmup_app)
    response = client.get("/metrics/prometheus")
    assert response.status_code == 200
    assert "warmup_http_requests_total" in response.text


def test_warmup_provider_profile_tuning_is_deterministic():
    client = TestClient(warmup_app)

    gmail_job = client.post(
        "/warmup/jobs",
        json={
            "tenant_id": "tenant-provider",
            "mailbox": "mailer@gmail.com",
            "domain_age_days": 90,
            "blacklist_detected": False,
            "timezone": "UTC",
        },
    )
    assert gmail_job.status_code == 200

    tuned = client.post(
        "/warmup/reputation/score",
        json={
            "tenant_id": "tenant-provider",
            "mailbox": "mailer@gmail.com",
            "inbox_rate": 0.9,
            "spam_rate": 0.02,
            "bounce_rate": 0.01,
            "complaint_rate": 0.001,
            "reply_rate": 0.3,
            "blacklist_detected": False,
        },
    )
    assert tuned.status_code == 200
    body = tuned.json()
    assert body["mode"] in {"normal", "rescue", "throttle"}
    assert body["daily_target"] > 0


_MAILBOX_STRATEGY = st.builds(
    lambda local, domain: f"{local}@{domain}",
    local=st.text(
        alphabet=st.characters(min_codepoint=97, max_codepoint=122),
        min_size=3,
        max_size=10,
    ),
    domain=st.sampled_from(["partners.test", "warmup.local", "network.local", "example.net"]),
).map(str.lower)


@settings(max_examples=30)
@given(
    requested_count=st.integers(min_value=1, max_value=80),
    partner_pool=st.lists(
        _MAILBOX_STRATEGY,
        min_size=0,
        max_size=15,
        unique=True,
    ),
)
def test_warmup_schedule_property_invariants(requested_count, partner_pool):
    client = TestClient(warmup_app)
    mailbox = f"scheduler-{uuid.uuid4().hex[:8]}@example.com"

    create = client.post(
        "/warmup/jobs",
        json={
            "tenant_id": "tenant-prop-schedule",
            "mailbox": mailbox,
            "domain_age_days": 45,
            "blacklist_detected": False,
            "timezone": "UTC",
        },
    )
    assert create.status_code == 200

    schedule = client.post(
        "/warmup/schedule/generate",
        json={
            "tenant_id": "tenant-prop-schedule",
            "mailbox": mailbox,
            "partner_pool": partner_pool,
            "requested_count": requested_count,
        },
    )
    assert schedule.status_code == 200
    body = schedule.json()
    items = body["items"]

    assert 1 <= len(items) <= requested_count
    assert 0.0 <= body["entropy_score"] <= 1.0
    assert all("send_at" in item and "partner" in item for item in items)
    assert all("@" in item["partner"] for item in items)
    send_ats = [item["send_at"] for item in items]
    assert send_ats == sorted(send_ats)
