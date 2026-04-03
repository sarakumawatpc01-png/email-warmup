# email-warmup

This repository uses the supplement-style monorepo naming from:

- `MasterBuildPlan_v2_Complete.docx`
- `EmailSaaS_Supplement_v1.docx`

while preserving current functionality.

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
- `nginx/` (reverse proxy and secure headers)
- `onboarding-scripts/`
  - `provision_client.sh`
  - `deprovision_client.sh`
- `tests/` → backend smoke tests

## Environment

Copy `.env.example` to `.env` and update values.

## Run with Docker Compose

```bash
docker compose up --build -d
```

## Access URLs

- Client UI dashboard: `http://localhost/`
- Superadmin control plane: `http://localhost/admin/`
- API gateway base (through nginx): `http://localhost/api/`

## First-Time Login / Signup

### Superadmin bootstrap

1. Open `http://localhost/admin/`
2. Use **Signup Superadmin** to create your first superadmin account:
   - email
   - password (min 8 chars)
   - tenant_id (use `system` for initial setup)
3. After signup you are logged in automatically and can access:
   - Internal Network IDs
   - Client Mailboxes
   - Health & Analytics
   - Payment Gateway Setup
   - Billing Admin
   - Audit Logs

### Client account

1. Open `http://localhost/`
2. Use **Signup** to create a client account:
   - email
   - password (min 8 chars)
   - tenant_id
3. After signup you are logged in automatically and can use dashboard quick actions.

### Existing users

- Use **Login** on either UI with your existing credentials.
- Use **Logout** in the top-right to clear local session.

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

## Notes

- Frontend requests are routed through nginx as `/api/*`.
- Auth users are currently in-memory in auth service, so accounts are not persistent across auth service restarts yet.

## Tests

```bash
python -m pip install -r requirements.txt
pytest -q
```
