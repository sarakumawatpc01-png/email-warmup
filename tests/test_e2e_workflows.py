import importlib.util
import os
import shutil
import sys
import tempfile
import uuid
from pathlib import Path

from fastapi.testclient import TestClient


def load_module(module_path: str):
    module_name = f"e2e_module_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_end_to_end_auth_lead_warmup_and_admin_audit_flow():
    root = Path(__file__).resolve().parents[1]
    temp_dir = Path(tempfile.mkdtemp(prefix="email-warmup-e2e-"))
    try:
        os.environ["AUTH_STATE_DB_PATH"] = str(temp_dir / "auth.db")
        os.environ["DATABASE_URL"] = f"sqlite+pysqlite:///{temp_dir / 'lead.db'}"
        os.environ["WARMUP_DATABASE_URL"] = f"sqlite+pysqlite:///{temp_dir / 'warmup.db'}"
        os.environ["WARMUP_SQLITE_FALLBACK_URL"] = f"sqlite+pysqlite:///{temp_dir / 'warmup-fallback.db'}"

        auth_mod = load_module(str(root / "backend/services/auth/app/main.py"))
        lead_mod = load_module(str(root / "backend/services/lead-service/app/main.py"))
        warmup_mod = load_module(str(root / "warmup-engine/app/main.py"))

        auth_client = TestClient(auth_mod.app)
        lead_client = TestClient(lead_mod.app)
        warmup_client = TestClient(warmup_mod.app)

        signup = auth_client.post(
            "/signup",
            json={
                "email": "tenant-admin@example.com",
                "password": "StrongPass123",
                "role": "tenant_admin",
                "tenant_id": "tenant-e2e",
            },
        )
        assert signup.status_code == 200
        access_token = signup.json()["access_token"]

        lead = lead_client.post(
            "/leads",
            headers={"Authorization": f"Bearer {access_token}"},
            json={"email": "lead1@example.com", "company": "Acme"},
        )
        assert lead.status_code == 200

        bulk = lead_client.post(
            "/leads/bulk",
            headers={"Authorization": f"Bearer {access_token}"},
            json=[
                {"email": "lead2@example.com"},
                {"email": "lead2@example.com"},
                {"email": "lead3@example.com"},
            ],
        )
        assert bulk.status_code == 200
        assert bulk.json()["created"] == 2
        assert bulk.json()["rejected"] == 1

        job = warmup_client.post(
            "/warmup/jobs",
            json={
                "tenant_id": "tenant-e2e",
                "mailbox": "tenant-admin@example.com",
                "domain_age_days": 30,
                "blacklist_detected": False,
            },
        )
        assert job.status_code == 200
        assert job.json()["job_id"].startswith("warmup-")

        service_token = auth_client.post(
            "/service/token",
            json={
                "service_name": "billing-service",
                "actions": ["admin", "read"],
                "resources": ["warmup"],
                "bootstrap_token": "dev-service-bootstrap",
            },
        )
        assert service_token.status_code == 200
        token = service_token.json()["access_token"]

        headers = {
            "Authorization": f"Bearer {token}",
            "x-caller-service": "gateway",
            "x-caller-identity": "spiffe://email-warmup/gateway",
            "x-target-service": "warmup",
            "x-request-id": f"req-{uuid.uuid4().hex[:12]}",
            "x-gateway-signed-at": "1700000000",
            "x-gateway-signature": "invalid",
        }
        denied = warmup_client.get("/warmup/admin/audit-logs", headers=headers)
        assert denied.status_code == 403
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
