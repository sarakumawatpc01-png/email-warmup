import hashlib
import hmac
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import jwt
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, Field

app = FastAPI(title="Auth Service", version="1.0.0")

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
RETURN_RESET_OTP = os.getenv("RETURN_RESET_OTP", "false").lower() == "true"
OTP_LENGTH = int(os.getenv("OTP_LENGTH", "8"))
REFRESH_TOKEN_EXPIRE_MINUTES = int(os.getenv("REFRESH_TOKEN_EXPIRE_MINUTES", "10080"))
SERVICE_BOOTSTRAP_TOKEN = os.getenv("SERVICE_BOOTSTRAP_TOKEN", "dev-service-bootstrap")
AUTH_STATE_DB_PATH = os.getenv("AUTH_STATE_DB_PATH", "./auth_state.db")
DEPLOY_ENV = os.getenv("DEPLOY_ENV", "dev").lower()
PASSWORD_HASH_ITERATIONS = int(os.getenv("PASSWORD_HASH_ITERATIONS", "390000"))


def _is_non_production_env() -> bool:
    return DEPLOY_ENV in {"dev", "test", "local"}


def _enforce_runtime_secrets() -> None:
    if _is_non_production_env():
        return
    if JWT_SECRET == "dev-secret":
        raise RuntimeError("JWT_SECRET must be set in non-dev environments")
    if SERVICE_BOOTSTRAP_TOKEN == "dev-service-bootstrap":
        raise RuntimeError("SERVICE_BOOTSTRAP_TOKEN must be set in non-dev environments")

users: Dict[str, dict] = {}
reset_otps: Dict[str, dict] = {}
refresh_sessions: Dict[str, str] = {}
revoked_token_ids: set[str] = set()
revoked_session_ids: set[str] = set()

POLICY_MATRIX: dict[str, list[str]] = {
    "warmup.admin": ["*", "warmup:admin"],
    "warmup.read": ["*", "warmup:read", "warmup:admin"],
    "billing.providers.manage": ["*", "billing:manage_providers"],
    "billing.providers.read": ["*", "billing:manage_providers", "billing:read_providers"],
}


def _db_connection() -> sqlite3.Connection:
    return sqlite3.connect(AUTH_STATE_DB_PATH)


def _init_state_store() -> None:
    with _db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS revoked_tokens (
                jti TEXT PRIMARY KEY,
                revoked_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS revoked_sessions (
                sid TEXT PRIMARY KEY,
                revoked_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_sessions (
                sid TEXT PRIMARY KEY,
                jti TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        revoked_token_ids.update(row[0] for row in conn.execute("SELECT jti FROM revoked_tokens"))
        revoked_session_ids.update(row[0] for row in conn.execute("SELECT sid FROM revoked_sessions"))
        refresh_sessions.update({row[0]: row[1] for row in conn.execute("SELECT sid, jti FROM refresh_sessions")})


def _persist_refresh_session(sid: str, jti: str) -> None:
    refresh_sessions[sid] = jti
    with _db_connection() as conn:
        conn.execute(
            """
            INSERT INTO refresh_sessions (sid, jti, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(sid) DO UPDATE SET jti=excluded.jti, updated_at=excluded.updated_at
            """,
            (sid, jti, datetime.now(timezone.utc).isoformat()),
        )


def _revoke_jti(jti: str) -> None:
    revoked_token_ids.add(jti)
    with _db_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO revoked_tokens (jti, revoked_at) VALUES (?, ?)",
            (jti, datetime.now(timezone.utc).isoformat()),
        )


def _revoke_sid(sid: str) -> None:
    revoked_session_ids.add(sid)
    refresh_sessions.pop(sid, None)
    with _db_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO revoked_sessions (sid, revoked_at) VALUES (?, ?)",
            (sid, datetime.now(timezone.utc).isoformat()),
        )
        conn.execute("DELETE FROM refresh_sessions WHERE sid = ?", (sid,))


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


class RevokeRequest(BaseModel):
    token: str | None = None
    session_id: str | None = None


class RefreshRequest(BaseModel):
    refresh_token: str


class ServiceTokenRequest(BaseModel):
    service_name: str = Field(min_length=2, max_length=64)
    actions: list[str] = Field(default_factory=list, max_length=32)
    resources: list[str] = Field(default_factory=list, max_length=32)
    bootstrap_token: str


class AuthorizeRequest(BaseModel):
    token: str
    action: str = Field(min_length=2, max_length=64)
    resource: str = Field(min_length=2, max_length=128)
    tenant_scope: str | None = None


def hash_password(password: str, salt: str) -> str:
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PASSWORD_HASH_ITERATIONS,
    )
    return f"pbkdf2_sha256${PASSWORD_HASH_ITERATIONS}${derived.hex()}"


def verify_password(password: str, user: dict) -> bool:
    salt = user["salt"]
    stored_hash = user.get("password_hash", "")
    if isinstance(stored_hash, str) and stored_hash.startswith("pbkdf2_sha256$"):
        _, iterations, expected = stored_hash.split("$", 2)
        derived = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            int(iterations),
        ).hex()
        return hmac.compare_digest(derived, expected)

    legacy = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
    if hmac.compare_digest(legacy, str(stored_hash)):
        user["password_hash"] = hash_password(password, salt)
        return True
    return False


def _new_jti() -> str:
    return secrets.token_hex(16)


def _policy_allows(claims: dict[str, Any], action: str, resource: str, tenant_scope: str | None = None) -> bool:
    permissions = claims.get("permissions", [])
    if not isinstance(permissions, list):
        return False
    if claims.get("sid") in revoked_session_ids or claims.get("jti") in revoked_token_ids:
        return False
    if (
        tenant_scope
        and claims.get("role") != "superadmin"
        and claims.get("token_type") != "service"
        and claims.get("tenant_id") != tenant_scope
    ):
        return False
    matrix_key = f"{resource}.{action}"
    allowed = POLICY_MATRIX.get(matrix_key, [])
    return "*" in permissions or any(scope in permissions for scope in allowed)


def issue_token(
    email: str,
    role: str,
    tenant_id: str,
    *,
    sid: str,
    token_type: str = "access",
    permissions: list[str] | None = None,
    expires_minutes: int = ACCESS_TOKEN_EXPIRE_MINUTES,
) -> str:
    role_permissions = {
        "superadmin": ["*"],
        "tenant_admin": ["warmup:admin", "warmup:read", "billing:manage_providers", "billing:read_providers"],
        "client": ["warmup:read", "billing:read_providers"],
    }
    now = datetime.now(timezone.utc)
    payload = {
        "jti": _new_jti(),
        "sid": sid,
        "sub": email,
        "role": role,
        "tenant_id": tenant_id,
        "permissions": permissions if permissions is not None else role_permissions.get(role, []),
        "token_type": token_type,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=expires_minutes)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def issue_access_refresh_pair(email: str, role: str, tenant_id: str, sid: str | None = None) -> dict[str, str]:
    session_id = sid or secrets.token_hex(12)
    access_token = issue_token(email, role, tenant_id, sid=session_id)
    refresh_token = issue_token(
        email,
        role,
        tenant_id,
        sid=session_id,
        token_type="refresh",
        expires_minutes=REFRESH_TOKEN_EXPIRE_MINUTES,
    )
    refresh_claims = jwt.decode(refresh_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    _persist_refresh_session(session_id, str(refresh_claims.get("jti", "")))
    return {"access_token": access_token, "refresh_token": refresh_token, "session_id": session_id}


def revoke_token_or_session(token: str | None = None, session_id: str | None = None) -> bool:
    revoked = False
    if token:
        try:
            claims = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM], options={"verify_exp": False})
            jti = claims.get("jti")
            sid = claims.get("sid")
            if isinstance(jti, str):
                _revoke_jti(jti)
                revoked = True
            if isinstance(sid, str):
                _revoke_sid(sid)
                revoked = True
        except jwt.InvalidTokenError:
            return False
    if session_id:
        _revoke_sid(session_id)
        revoked = True
    return revoked


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
    tokens = issue_access_refresh_pair(key, payload.role, payload.tenant_id)
    return {**tokens, "token_type": "bearer"}


@app.post("/login")
def login(payload: LoginRequest) -> dict:
    key = payload.email.lower()
    user = users.get(key)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not verify_password(payload.password, user):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    tokens = issue_access_refresh_pair(user["email"], user["role"], user["tenant_id"])
    return {**tokens, "token_type": "bearer"}


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
    if claims.get("sid") in revoked_session_ids or claims.get("jti") in revoked_token_ids:
        raise HTTPException(status_code=401, detail="Token revoked")
    if claims.get("token_type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")
    return {
        "email": claims.get("sub"),
        "role": claims.get("role"),
        "tenant_id": claims.get("tenant_id"),
        "permissions": claims.get("permissions", []),
        "session_id": claims.get("sid"),
    }


@app.post("/token/revoke")
def revoke_token(payload: RevokeRequest) -> dict:
    if not payload.token and not payload.session_id:
        raise HTTPException(status_code=400, detail="token or session_id required")
    revoked = revoke_token_or_session(payload.token, payload.session_id)
    if not revoked:
        raise HTTPException(status_code=400, detail="Unable to revoke")
    return {"status": "revoked", "session_id": payload.session_id}


@app.post("/token/logout")
def logout(payload: RevokeRequest) -> dict:
    return revoke_token(payload)


@app.post("/token/refresh")
def refresh_token(payload: RefreshRequest) -> dict:
    try:
        claims = jwt.decode(payload.refresh_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid refresh token") from exc

    if claims.get("token_type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token type")

    sid = claims.get("sid")
    jti = claims.get("jti")
    if not isinstance(sid, str) or not isinstance(jti, str):
        raise HTTPException(status_code=401, detail="Invalid refresh token claims")
    current_jti = refresh_sessions.get(sid)
    if sid in revoked_session_ids:
        raise HTTPException(status_code=401, detail="Session revoked")
    if jti in revoked_token_ids:
        _revoke_sid(sid)
        raise HTTPException(status_code=401, detail="Session revoked")
    if current_jti != jti:
        _revoke_sid(sid)
        raise HTTPException(status_code=401, detail="Refresh replay detected")

    _revoke_jti(jti)
    tokens = issue_access_refresh_pair(claims.get("sub", ""), claims.get("role", "client"), claims.get("tenant_id", ""), sid=sid)
    return {**tokens, "token_type": "bearer", "rotated": True}


@app.post("/authorize")
def authorize(payload: AuthorizeRequest) -> dict:
    try:
        claims = jwt.decode(payload.token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    allowed = _policy_allows(claims, payload.action, payload.resource, payload.tenant_scope)
    return {"allowed": allowed, "action": payload.action, "resource": payload.resource}


@app.post("/service/token")
def issue_service_token(payload: ServiceTokenRequest) -> dict:
    if payload.bootstrap_token != SERVICE_BOOTSTRAP_TOKEN:
        raise HTTPException(status_code=403, detail="Bootstrap token denied")
    if not payload.actions or not payload.resources:
        raise HTTPException(status_code=400, detail="actions and resources are required")
    permissions = [f"{resource}:{action}" for action in payload.actions for resource in payload.resources]
    permissions = list(dict.fromkeys(permissions))
    sid = f"svc-{payload.service_name}-{secrets.token_hex(6)}"
    token = issue_token(
        email=f"service:{payload.service_name}",
        role="superadmin",
        tenant_id="system",
        sid=sid,
        token_type="service",
        permissions=permissions,
        expires_minutes=60,
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "identity": {
            "type": "service",
            "service_name": payload.service_name,
            "spiffe_id": f"spiffe://email-warmup/{payload.service_name}",
            "mTLS_required": True,
        },
    }


_enforce_runtime_secrets()
_init_state_store()
