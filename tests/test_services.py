from fastapi.testclient import TestClient

from services.auth.app.main import app as auth_app
from services.warmup-engine.app.main import app as warmup_app
from services.verification-engine.app.main import app as verification_app


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
