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
- `onboarding-scripts/`
  - `provision_client.sh`
  - `deprovision_client.sh`
- `tests/` → backend smoke tests

## Environment

Copy `.env.example` to `.env` and update values.

### Required production values

- `TRAEFIK_HOST=email-warmup.agencyfic.com` (or your domain)
- `ALLOWED_ORIGINS=https://email-warmup.agencyfic.com`
- `POSTGRES_PASSWORD=<strong-secret>`
- `JWT_SECRET=<strong-random-secret>`
- `GRAFANA_ADMIN_PASSWORD=<strong-secret>`
- `WARMUP_DATABASE_URL` must use the same DB password as `POSTGRES_PASSWORD`

## Run with Docker Compose

```bash
docker compose up --build -d
```

## Traefik Routing (production)

- Domain: `https://email-warmup.agencyfic.com`
- `/` → client UI (`frontend-client`)
- `/admin/` → superadmin UI (`frontend-admin`)
- `/api/*` → backend gateway (`backend`)
- direct API paths (`/auth`, `/leads`, `/verification`, `/warmup`, `/ai`, `/billing`, `/whatsapp`, `/policy`, `/health`) → backend gateway (`backend`)

`docker-compose.yml` expects an external Docker network named `proxy` for Traefik integration.
Ports `80` and `443` must be owned by Traefik only; this stack does not publish host ports for app containers.

### Required public service labels (frontend client)

The public-facing client service uses these required labels:

- `traefik.enable=true`
- `traefik.http.routers.email-warmup.rule=Host(\`email-warmup.agencyfic.com\`)`
- `traefik.http.routers.email-warmup.entrypoints=websecure`
- `traefik.http.routers.email-warmup.tls.certresolver=letsencrypt`
- `traefik.http.services.email-warmup.loadbalancer.server.port=80`

## Production go-live (Traefik server)

1. **Prepare server**
   - Install Docker + Docker Compose plugin.
   - Ensure Traefik is already running on the server with `websecure` and Let’s Encrypt enabled.
   - Ensure external Docker network exists:
     ```bash
     docker network create proxy || true
     ```

2. **Deploy app**
   - Create `.env` from `.env.example` and set secure production values.
   - Start stack:
     ```bash
     docker compose pull
     docker compose up -d --build
     ```

3. **Validate runtime**
   - Check service state:
     ```bash
     docker compose ps
     ```
   - Check logs for any failed service:
     ```bash
     docker compose logs --tail=200 <service_name>
     ```

4. **Validate public routes**
   - `https://email-warmup.agencyfic.com/` → client UI
   - `https://email-warmup.agencyfic.com/admin/` → superadmin UI
   - `https://email-warmup.agencyfic.com/health` (or `/api/health`) → backend health path

5. **Post-deploy checks**
   - Confirm DNS A record points domain to server IP.
   - Confirm Traefik issued TLS certificate.
   - Confirm internal services are not publicly exposed by host ports.
   - Confirm external `proxy` network exists and Traefik sees `email-warmup`, `email-warmup-admin`, and API routers.

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
