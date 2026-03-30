import importlib.util
from pathlib import Path

from fastapi.testclient import TestClient


def load_app(module_path: str):
    spec = importlib.util.spec_from_file_location("service_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module.app


ROOT = Path(__file__).resolve().parents[1]
auth_app = load_app(str(ROOT / "services/auth/app/main.py"))
warmup_app = load_app(str(ROOT / "services/warmup-engine/app/main.py"))
verification_app = load_app(str(ROOT / "services/verification-engine/app/main.py"))


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
