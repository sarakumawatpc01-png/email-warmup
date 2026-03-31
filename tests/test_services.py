import importlib.util
import atexit
import shutil
import os
import sys
import tempfile
import time
import uuid
import hmac
import hashlib
from pathlib import Path

import jwt
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
TMP_DIR = Path(tempfile.mkdtemp(prefix="email-warmup-tests-"))
atexit.register(shutil.rmtree, TMP_DIR, True)
os.environ.setdefault("AUTH_STATE_DB_PATH", str(TMP_DIR / f"email-warmup-auth-state-{uuid.uuid4().hex}.db"))
os.environ.setdefault("DATABASE_URL", f"sqlite+pysqlite:///{TMP_DIR / f'email-warmup-lead-{uuid.uuid4().hex}.db'}")
auth_app = load_app(str(ROOT / "backend/services/auth/app/main.py"))
warmup_app = load_app(str(ROOT / "warmup-engine/app/main.py"))
verification_app = load_app(str(ROOT / "verification-engine/app/main.py"))
lead_app = load_app(str(ROOT / "backend/services/lead-service/app/main.py"))
gateway_app = load_app(str(ROOT / "backend/app/main.py"))


def gateway_signed_headers(token: str, request_id: str, signed_at: int | None = None) -> dict:
    ts = str(signed_at if signed_at is not None else int(time.time()))
    canonical = "|".join(
        [
            "gateway",
            "spiffe://email-warmup/gateway",
            "warmup",
            request_id,
            ts,
        ]
    )
    signature = hmac.new(
        b"dev-gateway-signing-secret",
        canonical.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "Authorization": f"Bearer {token}",
        "x-caller-service": "gateway",
        "x-caller-identity": "spiffe://email-warmup/gateway",
        "x-target-service": "warmup",
        "x-request-id": request_id,
        "x-gateway-signed-at": ts,
        "x-gateway-signature": signature,
    }


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


def test_auth_session_revoke_invalidates_verify():
    client = TestClient(auth_app)
    signup = client.post(
        "/signup",
        json={
            "email": f"revoke-{uuid.uuid4().hex[:6]}@example.com",
            "password": "StrongPass123",
            "role": "client",
            "tenant_id": "tenant-revoke",
        },
    )
    assert signup.status_code == 200
    token = signup.json()["access_token"]
    session_id = signup.json()["session_id"]

    revoke = client.post("/token/revoke", json={"session_id": session_id})
    assert revoke.status_code == 200

    verify = client.post("/verify-token", json={"token": token})
    assert verify.status_code == 401


def test_auth_refresh_replay_detection_revokes_session():
    client = TestClient(auth_app)
    signup = client.post(
        "/signup",
        json={
            "email": f"refresh-{uuid.uuid4().hex[:6]}@example.com",
            "password": "StrongPass123",
            "role": "client",
            "tenant_id": "tenant-refresh",
        },
    )
    assert signup.status_code == 200
    refresh_token = signup.json()["refresh_token"]

    first = client.post("/token/refresh", json={"refresh_token": refresh_token})
    assert first.status_code == 200

    replay = client.post("/token/refresh", json={"refresh_token": refresh_token})
    assert replay.status_code == 401
    assert replay.json()["detail"] == "Session revoked"

    post_replay = client.post("/token/refresh", json={"refresh_token": first.json()["refresh_token"]})
    assert post_replay.status_code == 401
    assert post_replay.json()["detail"] == "Session revoked"


def test_auth_service_token_can_authorize_warmup_admin():
    client = TestClient(auth_app)
    issued = client.post(
        "/service/token",
        json={
            "service_name": "warmup-engine",
            "actions": ["admin"],
            "resources": ["warmup"],
            "bootstrap_token": "dev-service-bootstrap",
        },
    )
    assert issued.status_code == 200
    token = issued.json()["access_token"]

    decision = client.post(
        "/authorize",
        json={"token": token, "action": "admin", "resource": "warmup", "tenant_scope": "tenant-x"},
    )
    assert decision.status_code == 200
    assert decision.json()["allowed"] is True


def test_auth_service_token_requires_actions_and_resources():
    client = TestClient(auth_app)
    issued = client.post(
        "/service/token",
        json={
            "service_name": "warmup-engine",
            "actions": [],
            "resources": [],
            "bootstrap_token": "dev-service-bootstrap",
        },
    )
    assert issued.status_code == 400


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


def test_warmup_visibility_timeout_sweeper_and_lease_renew():
    client = TestClient(warmup_app)
    key = f"lease-{uuid.uuid4().hex[:16]}"
    enqueue = client.post(
        "/warmup/worker/enqueue",
        json={
            "tenant_id": "tenant-lease",
            "mailbox": "lease@example.com",
            "queue_name": "send_execution",
            "idempotency_key": key,
            "payload": {"force_fail": False},
            "max_attempts": 2,
        },
    )
    assert enqueue.status_code == 200
    event_id = enqueue.json()["event_id"]

    inflight_route = next(route for route in warmup_app.router.routes if getattr(route, "path", "") == "/warmup/worker/inflight")
    warmup_state = inflight_route.endpoint.__globals__
    with warmup_state["Session"](warmup_state["engine"]) as session:
        session.add(
            warmup_state["WorkerLease"](
                event_id=event_id,
                queue_name="send_execution",
                lease_until=warmup_state["utc_now"]() - warmup_state["timedelta"](seconds=5),
            )
        )
        session.commit()

    sweep = client.post("/warmup/worker/sweep-stuck")
    assert sweep.status_code in {200, 403}

    if sweep.status_code == 200:
        assert sweep.json()["swept"] is True
        assert sweep.json()["requeued"] >= 1

    renew = client.post(
        "/warmup/worker/lease/renew",
        json={"event_id": event_id, "queue_name": "send_execution", "extend_seconds": 30},
    )
    assert renew.status_code in {200, 404}


def test_warmup_admin_tenant_scope_denied_for_tenant_admin():
    warmup_client = TestClient(warmup_app)
    auth_client = TestClient(auth_app)
    signup = auth_client.post(
        "/signup",
        json={
            "email": f"tenantadmin-scope-{uuid.uuid4().hex[:6]}@example.com",
            "password": "StrongPass123",
            "role": "tenant_admin",
            "tenant_id": "tenant-scope-a",
        },
    )
    assert signup.status_code == 200
    token = signup.json()["access_token"]

    denied = warmup_client.post(
        "/warmup/admin/internal-mailboxes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "tenant_id": "tenant-scope-b",
            "mailbox": f"seed-{uuid.uuid4().hex[:6]}@gmail.com",
            "notes": "scope-check",
        },
    )
    assert denied.status_code == 403


def test_warmup_outbox_and_inbox_idempotency():
    client = TestClient(warmup_app)
    idem = f"outbox-{uuid.uuid4().hex[:16]}"
    enqueue = client.post(
        "/warmup/worker/enqueue",
        json={
            "tenant_id": "tenant-outbox",
            "mailbox": "outbox@example.com",
            "queue_name": "send_execution",
            "idempotency_key": idem,
            "payload": {"force_fail": False},
            "max_attempts": 2,
        },
    )
    assert enqueue.status_code == 200

    outbox = client.get("/warmup/outbox/pending")
    assert outbox.status_code == 200
    assert any(item["dedupe_key"] == f"warmup-event:{enqueue.json()['event_id']}" for item in outbox.json()["items"])

    first_inbox = client.post(
        "/warmup/inbox/record",
        json={"source": "billing-webhook", "message_id": f"msg-{uuid.uuid4().hex[:12]}", "payload": {"type": "charge.succeeded"}},
    )
    assert first_inbox.status_code == 200
    assert first_inbox.json()["idempotent"] is False

    second_inbox = client.post(
        "/warmup/inbox/record",
        json={"source": "billing-webhook", "message_id": first_inbox.json()["message_id"], "payload": {"type": "charge.succeeded"}},
    )
    assert second_inbox.status_code == 200
    assert second_inbox.json()["idempotent"] is True


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


def test_lead_requires_permissions_for_create():
    lead_client = TestClient(lead_app)
    now = int(time.time())
    token = jwt.encode(
        {
            "sub": "user@example.com",
            "tenant_id": "tenant-no-perm",
            "permissions": [],
            "token_type": "access",
            "iat": now - 60,
            "exp": now + 600,
        },
        "dev-secret",
        algorithm="HS256",
    )
    denied = lead_client.post(
        "/leads",
        headers={"Authorization": f"Bearer {token}"},
        json={"email": "lead@example.com"},
    )
    assert denied.status_code == 403


def test_lead_rejects_revoked_session_token():
    auth_client = TestClient(auth_app)
    lead_client = TestClient(lead_app)
    signup = auth_client.post(
        "/signup",
        json={
            "email": f"leadrevoke-{uuid.uuid4().hex[:6]}@example.com",
            "password": "StrongPass123",
            "role": "client",
            "tenant_id": "tenant-lead-revoke",
        },
    )
    assert signup.status_code == 200
    token = signup.json()["access_token"]
    session_id = signup.json()["session_id"]

    created = lead_client.post(
        "/leads",
        headers={"Authorization": f"Bearer {token}"},
        json={"email": f"before-revoke-{uuid.uuid4().hex[:6]}@example.com"},
    )
    assert created.status_code == 200

    logout = auth_client.post("/token/logout", json={"session_id": session_id})
    assert logout.status_code == 200

    denied = lead_client.post(
        "/leads",
        headers={"Authorization": f"Bearer {token}"},
        json={"email": f"after-revoke-{uuid.uuid4().hex[:6]}@example.com"},
    )
    assert denied.status_code == 401
    assert denied.json()["detail"] == "Token revoked"


def test_warmup_service_token_requires_gateway_context_headers():
    auth_client = TestClient(auth_app)
    warmup_client = TestClient(warmup_app)
    issued = auth_client.post(
        "/service/token",
        json={
            "service_name": "billing-service",
            "actions": ["admin"],
            "resources": ["warmup"],
            "bootstrap_token": "dev-service-bootstrap",
        },
    )
    assert issued.status_code == 200
    token = issued.json()["access_token"]

    denied = warmup_client.get(
        "/warmup/admin/audit-logs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert denied.status_code == 403
    assert denied.json()["detail"] == "Gateway service context required"

    allowed = warmup_client.get(
        "/warmup/admin/audit-logs",
        headers=gateway_signed_headers(token, request_id=f"req-{uuid.uuid4().hex[:12]}"),
    )
    assert allowed.status_code == 200


def test_warmup_superadmin_service_token_still_needs_gateway_context():
    auth_client = TestClient(auth_app)
    warmup_client = TestClient(warmup_app)
    issued = auth_client.post(
        "/service/token",
        json={
            "service_name": "warmup-engine",
            "actions": ["admin"],
            "resources": ["warmup"],
            "bootstrap_token": "dev-service-bootstrap",
        },
    )
    assert issued.status_code == 200
    token = issued.json()["access_token"]

    denied = warmup_client.get(
        "/warmup/admin/audit-logs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert denied.status_code == 403
    assert denied.json()["detail"] == "Gateway service context required"


def test_warmup_service_token_rejects_bad_gateway_signature():
    auth_client = TestClient(auth_app)
    warmup_client = TestClient(warmup_app)
    issued = auth_client.post(
        "/service/token",
        json={
            "service_name": "billing-service",
            "actions": ["admin"],
            "resources": ["warmup"],
            "bootstrap_token": "dev-service-bootstrap",
        },
    )
    assert issued.status_code == 200
    token = issued.json()["access_token"]

    headers = gateway_signed_headers(token, request_id=f"req-{uuid.uuid4().hex[:12]}")
    headers["x-gateway-signature"] = "invalid-signature"
    denied = warmup_client.get("/warmup/admin/audit-logs", headers=headers)
    assert denied.status_code == 403
    assert denied.json()["detail"] == "Gateway signature invalid"


def test_warmup_authz_cache_ttl_expires():
    auth_client = TestClient(auth_app)
    warmup_client = TestClient(warmup_app)
    issued = auth_client.post(
        "/service/token",
        json={
            "service_name": "billing-service",
            "actions": ["admin"],
            "resources": ["warmup"],
            "bootstrap_token": "dev-service-bootstrap",
        },
    )
    assert issued.status_code == 200
    token = issued.json()["access_token"]
    request_id = f"req-{uuid.uuid4().hex[:12]}"
    headers = gateway_signed_headers(token, request_id=request_id)

    audit_route = next(route for route in warmup_app.router.routes if getattr(route, "path", "") == "/warmup/admin/audit-logs")
    warmup_state = audit_route.endpoint.__globals__
    cache = warmup_state["AUTHZ_CACHE"]
    cache._store.clear()
    cache.ttl_seconds = 1

    allowed = warmup_client.get("/warmup/admin/audit-logs", headers=headers)
    assert allowed.status_code == 200
    assert len(cache._store) >= 1
    cached = cache.get(token, "admin", "warmup", None)
    assert cached is True
    time.sleep(1.1)
    assert cache.get(token, "admin", "warmup", None) is None


def test_warmup_admin_audit_logs_include_correlation_id():
    auth_client = TestClient(auth_app)
    warmup_client = TestClient(warmup_app)
    issued = auth_client.post(
        "/service/token",
        json={
            "service_name": "billing-service",
            "actions": ["admin"],
            "resources": ["warmup"],
            "bootstrap_token": "dev-service-bootstrap",
        },
    )
    assert issued.status_code == 200
    token = issued.json()["access_token"]

    tenant = f"tenant-corr-{uuid.uuid4().hex[:6]}"
    mailbox = f"seed-{uuid.uuid4().hex[:6]}@gmail.com"
    corr_id = f"corr-{uuid.uuid4().hex[:12]}"
    upsert = warmup_client.post(
        "/warmup/admin/internal-mailboxes",
        headers=gateway_signed_headers(token, request_id=corr_id),
        json={"tenant_id": tenant, "mailbox": mailbox, "notes": "corr-test"},
    )
    assert upsert.status_code == 200

    logs = warmup_client.get(
        "/warmup/admin/audit-logs",
        headers=gateway_signed_headers(token, request_id=f"req-{uuid.uuid4().hex[:12]}"),
    )
    assert logs.status_code == 200
    assert any(
        item["action"] == "internal_mailbox_upsert"
        and item["resource_id"] == f"{tenant}:{mailbox.lower()}"
        and item["details"].get("correlation_id") == corr_id
        for item in logs.json()["items"]
    )


def test_gateway_proxy_adds_signature_headers_for_service_calls():
    gateway_client = TestClient(gateway_app)
    warmup_route = next(route for route in gateway_app.router.routes if getattr(route, "path", "") == "/warmup/{path:path}")
    gateway_state = warmup_route.endpoint.__globals__
    captured: dict[str, dict] = {}
    original_client = gateway_state["httpx"].AsyncClient

    class FakeResponse:
        status_code = 200
        headers = {"content-type": "application/json"}

        @staticmethod
        def json():
            return {"ok": True}

        text = '{"ok": true}'

    class FakeAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, method, url, headers, params, content):
            assert method == "GET"
            assert "/admin/audit-logs" in url
            assert params == {}
            captured["headers"] = headers
            return FakeResponse()

    gateway_state["httpx"].AsyncClient = FakeAsyncClient
    try:
        response = gateway_client.get("/warmup/admin/audit-logs")
        assert response.status_code == 200
        assert "x-gateway-signature" in captured["headers"]
        assert "x-gateway-signed-at" in captured["headers"]
    finally:
        gateway_state["httpx"].AsyncClient = original_client


def test_gateway_policy_consensus_requires_service_identity():
    gateway_client = TestClient(gateway_app)
    denied = gateway_client.post("/policy/consensus")
    assert denied.status_code == 403
    assert denied.json()["detail"] == "Forbidden"

    allowed = gateway_client.post("/policy/consensus", headers={"x-caller-service": "warmup"})
    assert allowed.status_code == 200
    assert allowed.json()["consensus_decision"] == "adopt"


def test_gateway_middleware_rejects_invalid_content_length():
    gateway_client = TestClient(gateway_app)
    invalid = gateway_client.post(
        "/policy/consensus",
        headers={"x-caller-service": "warmup", "content-length": "not-a-number"},
    )
    assert invalid.status_code == 400
    assert invalid.json()["detail"] == "Invalid Content-Length"


def test_gateway_rejects_oversized_proxy_body():
    gateway_client = TestClient(gateway_app)
    large = "x" * (11 * 1024 * 1024)
    response = gateway_client.post(
        "/auth/signup",
        content=large,
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 413
    assert response.json()["detail"] == "Request body too large"


def test_gateway_policy_rate_limit_applies():
    gateway_client = TestClient(gateway_app)
    route = next(route for route in gateway_app.router.routes if getattr(route, "path", "") == "/policy/consensus")
    gateway_state = route.endpoint.__globals__
    gateway_state["GATEWAY_ADMIN_RATE_LIMIT"] = 2
    gateway_state["GATEWAY_ADMIN_RATE_WINDOW_SECONDS"] = 60
    gateway_state["_RATE_LIMIT_BUCKETS"].clear()

    assert gateway_client.post("/policy/consensus", headers={"x-caller-service": "warmup"}).status_code == 200
    assert gateway_client.post("/policy/consensus", headers={"x-caller-service": "warmup"}).status_code == 200
    limited = gateway_client.post("/policy/consensus", headers={"x-caller-service": "warmup"})
    assert limited.status_code == 429


def test_auth_rate_limit_signup_applies():
    client = TestClient(auth_app)
    route = next(route for route in auth_app.router.routes if getattr(route, "path", "") == "/signup")
    state = route.endpoint.__globals__
    state["AUTH_SIGNUP_RATE_LIMIT"] = 1
    state["AUTH_RATE_WINDOW_SECONDS"] = 60
    state["_RATE_LIMIT_BUCKETS"].clear()

    payload = {
        "email": "rate-limit-signup@example.com",
        "password": "StrongPass123",
        "role": "client",
        "tenant_id": "tenant-rate",
    }
    first = client.post("/signup", json=payload)
    assert first.status_code == 200
    second = client.post("/signup", json=payload)
    assert second.status_code == 429


def test_lead_bulk_rate_limit_applies():
    auth_client = TestClient(auth_app)
    lead_client = TestClient(lead_app)
    route = next(route for route in lead_app.router.routes if getattr(route, "path", "") == "/leads/bulk")
    state = route.endpoint.__globals__
    state["LEAD_BULK_RATE_LIMIT"] = 1
    state["LEAD_BULK_RATE_WINDOW_SECONDS"] = 60
    state["_RATE_LIMIT_BUCKETS"].clear()

    signup = auth_client.post(
        "/signup",
        json={
            "email": f"bulk-rate-{uuid.uuid4().hex[:6]}@example.com",
            "password": "StrongPass123",
            "role": "tenant_admin",
            "tenant_id": "tenant-bulk-rate",
        },
    )
    token = signup.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    body = [{"email": f"{uuid.uuid4().hex[:6]}@example.com"}]
    assert lead_client.post("/leads/bulk", headers=headers, json=body).status_code == 200
    limited = lead_client.post("/leads/bulk", headers=headers, json=body)
    assert limited.status_code == 429


def test_warmup_admin_audit_export_and_filter():
    auth_client = TestClient(auth_app)
    warmup_client = TestClient(warmup_app)
    issued = auth_client.post(
        "/service/token",
        json={
            "service_name": "billing-service",
            "actions": ["admin"],
            "resources": ["warmup"],
            "bootstrap_token": "dev-service-bootstrap",
        },
    )
    token = issued.json()["access_token"]
    headers = gateway_signed_headers(token, request_id=f"req-{uuid.uuid4().hex[:12]}")

    export = warmup_client.get("/warmup/admin/audit-logs/export?limit=10", headers=headers)
    assert export.status_code == 200
    assert "text/csv" in export.headers.get("content-type", "")
    assert "created_at,actor,action,resource_type,resource_id,details" in export.text

    filtered = warmup_client.get("/warmup/admin/audit-logs?limit=5&action=internal_mailbox_upsert", headers=headers)
    assert filtered.status_code == 200
    assert "items" in filtered.json()

def test_warmup_phase_h_foundation_endpoints():
    client = TestClient(warmup_app)
    tenant = f"tenant-h-{uuid.uuid4().hex[:6]}"
    mailbox = f"h-{uuid.uuid4().hex[:6]}@gmail.com"

    seed = client.post(
        "/warmup/seed-mailboxes/ingest",
        json={
            "provider": "gmail",
            "folder": "inbox",
            "tenant_id": tenant,
            "mailbox": mailbox,
            "message_ids": ["m1", "m2"],
        },
    )
    assert seed.status_code == 200
    assert seed.json()["processed"] == 2

    feed = client.post(
        "/warmup/reputation/feeds/ingest",
        json={
            "provider": "gmail",
            "tenant_id": tenant,
            "mailbox": mailbox,
            "listed": True,
            "source": "rbl-test",
            "confidence": 0.8,
        },
    )
    assert feed.status_code == 200
    assert feed.json()["listed"] is True

    fingerprint = client.post(
        "/warmup/content/fingerprint",
        json={"tenant_id": tenant, "mailbox": mailbox, "subject": "Hello", "body": "Warmup content"},
    )
    assert fingerprint.status_code == 200
    assert len(fingerprint.json()["fingerprint"]) == 64

    control = client.post(
        "/warmup/slo/control-loop",
        json={
            "tenant_id": tenant,
            "mailbox": mailbox,
            "provider": "gmail",
            "send_success_ratio": 0.7,
            "placement_score": 0.6,
            "queue_latency_seconds": 180,
        },
    )
    assert control.status_code == 200
    assert control.json()["mode"] == "throttle"


def test_billing_service_phase_g_lifecycle_endpoints():
    billing_src = (ROOT / "billing-service/src/index.js").read_text(encoding="utf-8")
    assert "app.post('/orders'" in billing_src
    assert "app.get('/orders/:orderId'" in billing_src
    assert "app.get('/orders'" in billing_src
    assert "app.post('/orders/:orderId/cancel'" in billing_src
    assert "app.post('/subscriptions/start'" in billing_src
    assert "app.post('/subscriptions/:tenantId/upgrade'" in billing_src
    assert "app.post('/subscriptions/:tenantId/downgrade'" in billing_src
    assert "app.post('/subscriptions/:tenantId/cancel'" in billing_src
    assert "app.post('/orders/:orderId/refunds'" in billing_src
    assert "app.post('/orders/:orderId/disputes'" in billing_src


def test_billing_webhook_nonce_and_replay_guardrails():
    billing_src = (ROOT / "billing-service/src/index.js").read_text(encoding="utf-8")
    assert "x-webhook-timestamp" in billing_src
    assert "x-webhook-nonce" in billing_src
    assert "Webhook nonce replay detected" in billing_src
    assert "Webhook timestamp outside tolerance window" in billing_src


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
    tenant_id = f"tenant-prop-schedule-{uuid.uuid4().hex[:8]}"

    create = client.post(
        "/warmup/jobs",
        json={
            "tenant_id": tenant_id,
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
            "tenant_id": tenant_id,
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
