import re
import socket

import dns.resolver
from email_validator import EmailNotValidError, validate_email
from fastapi import FastAPI
from pydantic import BaseModel, EmailStr

app = FastAPI(title="Verification Engine", version="1.0.0")


class VerificationRequest(BaseModel):
    email: EmailStr


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "verification-engine"}


@app.post("/verify")
def verify(payload: VerificationRequest) -> dict:
    email = payload.email.lower()
    result = {
        "email": email,
        "syntax": False,
        "domain": False,
        "mx": False,
        "smtp": False,
        "status": "invalid",
    }

    try:
        validate_email(email, check_deliverability=False)
        result["syntax"] = True
    except EmailNotValidError:
        return result

    domain = email.split("@", 1)[1]
    if not re.match(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", domain):
        return result
    result["domain"] = True

    transient_issue = False
    try:
        answers = dns.resolver.resolve(domain, "MX")
        mx_hosts = [str(r.exchange).rstrip(".") for r in answers]
        result["mx"] = len(mx_hosts) > 0
    except Exception:
        transient_issue = True
        result["status"] = "unknown"
        return result

    for host in mx_hosts:
        try:
            with socket.create_connection((host, 25), timeout=2):
                result["smtp"] = True
                break
        except Exception:
            transient_issue = True
            continue

    if result["syntax"] and result["domain"] and result["mx"]:
        result["status"] = "catch_all_or_valid" if not result["smtp"] else "valid"
    elif transient_issue and result["syntax"] and result["domain"]:
        result["status"] = "unknown"

    return result
