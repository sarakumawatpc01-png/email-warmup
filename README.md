# email-warmup

This repository now follows the uploaded build docs (`MasterBuildPlan_v2_Complete.docx` and `EmailSaaS_Supplement_v1.docx`) with a production-style microservices baseline for an Email Marketing SaaS platform.

## Structure

- `services/` → backend services
  - `gateway` (FastAPI API gateway)
  - `auth` (JWT auth)
  - `lead-service` (tenant-scoped lead API + PostgreSQL)
  - `warmup-engine` (warm-up scheduling simulation)
  - `verification-engine` (syntax/domain/MX/SMTP verification pipeline)
  - `ai-agent` (action orchestration)
  - `billing` (Node.js + Stripe webhook endpoints)
  - `whatsapp-service` (Node.js WhatsApp campaign endpoints)
- `frontend/`
  - `frontend-client`
  - `frontend-admin`
- `infra/nginx` → reverse proxy and secure headers
- `scripts/provision_mautic_client.sh` → client Mautic provisioning stub
- `tests/` → backend smoke tests

## Environment

Copy `.env.example` to `.env` and update values.

## Run Locally

```bash
docker compose up --build
```

## Core Endpoints (via gateway)

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
