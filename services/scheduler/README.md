# scheduler

## Purpose

This service triggers ingestion on a fixed interval and exposes a lightweight ops API for run-health checks.

## API / Inputs / Outputs

- Scheduler loop output: runs ingestion worker command on interval.
- HTTP endpoints:
  - `GET /health`
  - `GET /ops/status`
- Ops status output includes failed run counts, trigger staleness, and stuck-run checks.

## Required Environment Variables

- `DATABASE_URL` (preferred) or `DB_PATH`
- `SCHEDULE_INTERVAL_SECONDS` (or `INTERVAL_SECONDS`)
- `ENABLE_API`
- `API_HOST`
- `API_PORT`

Common optional:

- `RUN_ONCE`
- `WORKER_ENTRYPOINT`
- `WORKER_ARGS`
- `OPS_MAX_FAILED_RUNS_24H`
- `OPS_STUCK_RUN_MINUTES`

## Local Run

From repo root:

```bash
DATABASE_URL=postgresql://grazeops:grazeops@localhost:5432/grazeops \
SCHEDULE_INTERVAL_SECONDS=300 \
ENABLE_API=1 \
API_PORT=8082 \
python3 services/scheduler/scheduler.py
```

## Smoke Check / Health

```bash
curl -s http://localhost:8082/health
curl -s http://localhost:8082/ops/status
```

## Dependencies

- Requires operational DB (`postgres-db` in compose).
- Runs ingestion worker entrypoint as a subprocess.
