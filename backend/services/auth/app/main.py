import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Dict

import jwt
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, Field

app = FastAPI(title="Auth Service", version="1.0.0")

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
RETURN_RESET_OTP = os.getenv("RETURN_RESET_OTP", "false").lower() == "true"
OTP_LENGTH = int(os.getenv("OTP_LENGTH", "8"))

users: Dict[str, dict] = {}
reset_otps: Dict[str, dict] = {}


class SignupRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    role: str = Field(pattern="^(superadmin|tenant_admin|client)$")
    tenant_id: str = Field(min_length=2, max_length=64)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class ResetRequest(BaseModel):
    email: EmailStr


class ResetConfirmRequest(BaseModel):
    email: EmailStr
    otp: str = Field(min_length=6, max_length=12)
    new_password: str = Field(min_length=8)


class VerifyRequest(BaseModel):
    token: str


def hash_password(password: str, salt: str) -> str:
    return hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()


def issue_token(email: str, role: str, tenant_id: str) -> str:
    role_permissions = {
        "superadmin": ["*"],
        "tenant_admin": ["warmup:admin", "warmup:read", "billing:manage_providers", "billing:read_providers"],
        "client": ["warmup:read", "billing:read_providers"],
    }
    now = datetime.now(timezone.utc)
    payload = {
        "sub": email,
        "role": role,
        "tenant_id": tenant_id,
        "permissions": role_permissions.get(role, []),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "auth"}


@app.post("/signup")
def signup(payload: SignupRequest) -> dict:
    key = payload.email.lower()
    if key in users:
        raise HTTPException(status_code=409, detail="User already exists")

    salt = secrets.token_hex(8)
    users[key] = {
        "email": key,
        "salt": salt,
        "password_hash": hash_password(payload.password, salt),
        "role": payload.role,
        "tenant_id": payload.tenant_id,
    }
    token = issue_token(key, payload.role, payload.tenant_id)
    return {"access_token": token, "token_type": "bearer"}


@app.post("/login")
def login(payload: LoginRequest) -> dict:
    key = payload.email.lower()
    user = users.get(key)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    computed = hash_password(payload.password, user["salt"])
    if not hmac.compare_digest(computed, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = issue_token(user["email"], user["role"], user["tenant_id"])
    return {"access_token": token, "token_type": "bearer"}


@app.post("/password-reset/request")
def password_reset_request(payload: ResetRequest) -> dict:
    key = payload.email.lower()
    if key not in users:
        return {"status": "accepted"}

    otp = "".join(secrets.choice("0123456789") for _ in range(OTP_LENGTH))
    reset_otps[key] = {
        "otp": otp,
        "expires_at": datetime.now(timezone.utc) + timedelta(minutes=10),
    }
    if RETURN_RESET_OTP:
        return {"status": "accepted", "otp": otp}
    return {"status": "accepted"}


@app.post("/password-reset/confirm")
def password_reset_confirm(payload: ResetConfirmRequest) -> dict:
    key = payload.email.lower()
    user = users.get(key)
    challenge = reset_otps.get(key)
    if not user or not challenge:
        raise HTTPException(status_code=400, detail="Invalid reset request")

    if datetime.now(timezone.utc) > challenge["expires_at"]:
        reset_otps.pop(key, None)
        raise HTTPException(status_code=400, detail="OTP expired")

    if not hmac.compare_digest(payload.otp, challenge["otp"]):
        raise HTTPException(status_code=400, detail="Invalid OTP")

    user["password_hash"] = hash_password(payload.new_password, user["salt"])
    reset_otps.pop(key, None)
    return {"status": "password_updated"}


@app.post("/verify-token")
def verify_token(payload: VerifyRequest) -> dict:
    try:
        claims = jwt.decode(payload.token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    return {
        "email": claims.get("sub"),
        "role": claims.get("role"),
        "tenant_id": claims.get("tenant_id"),
        "permissions": claims.get("permissions", []),
    }
