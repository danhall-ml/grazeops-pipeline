# postgres-db

## Purpose

This service runs PostgreSQL for the pipeline and initializes the assignment schema on startup.

## API / Inputs / Outputs

- Input: `init_postgres.sql` mounted into `/docker-entrypoint-initdb.d/01-init.sql`.
- Output: operational tables used by ingestion, calculation, scheduler, and audit flows.
- Network: PostgreSQL on port `5432` inside the compose network.

## Required Environment Variables

- `POSTGRES_DB` (default in compose: `grazeops`)
- `POSTGRES_USER` (default in compose: `grazeops`)
- `POSTGRES_PASSWORD` (default in compose: `grazeops`)

## Local Run

From repo root:

```bash
docker compose up -d postgres-db
```

## Smoke Check / Health

```bash
docker compose exec -T postgres-db pg_isready -U grazeops -d grazeops
```

## Dependencies

- No upstream service dependencies.
- Used by: `ingestion-worker`, `scheduler`, `calculation-service`, and `reviewer-ui`.
