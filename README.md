# email-warmup

This repository now uses the supplement-style monorepo naming from the uploaded docs (`MasterBuildPlan_v2_Complete.docx` and `EmailSaaS_Supplement_v1.docx`) while preserving current functionality.

## Structure (aligned)

- `backend/` → FastAPI API gateway (`backend/app/main.py`)
- `backend/services/`
  - `auth` (JWT auth)
  - `lead-service` (tenant-scoped lead API + PostgreSQL)
  - `whatsapp-service` (Node.js WhatsApp campaign endpoints)
- `warmup-engine/` (warm-up scheduling simulation)
- `verification-engine/` (syntax/domain/MX/SMTP verification pipeline)
- `ai-agent/` (action orchestration)
- `billing-service/` (Node.js + Stripe webhook endpoints)
- `frontend/`
  - `client`
  - `superadmin`
- `nginx/` → reverse proxy and secure headers
- `onboarding-scripts/`
  - `provision_client.sh`
  - `deprovision_client.sh`
- `tests/` → backend smoke tests

## Environment

Copy `.env.example` to `.env` and update values.

## Run Locally

```bash
docker compose up --build
```

## Core Endpoints (via backend gateway)

- `POST /auth/signup`
- `POST /auth/login`
- `POST /auth/password-reset/request`
- `POST /auth/password-reset/confirm`
- `POST /leads/leads`
- `GET /leads/leads`
- `POST /verification/verify`
- `POST /warmup/warmup/jobs`
- `POST /ai/run`
- `POST /billing/subscriptions/preview`
- `POST /whatsapp/messages`

## Tests

```bash
python -m pip install -r requirements.txt
pytest -q
```
