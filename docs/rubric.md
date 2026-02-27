# Part 1 Rubric Map

This file maps each Part 1 expectation to concrete implementation and where to verify it.

## Ingestion (Schedulable + Backfill via Date Ranges)

- Implemented in:
  - `services/ingestion-worker/main.py`
  - `services/ingestion-worker/ingestion_worker/worker.py`
  - `services/scheduler/scheduler.py`
- Evidence:
  - Ingestion accepts `--start-date` and `--end-date`.
  - Scheduler triggers ingestion on `SCHEDULE_INTERVAL_SECONDS`.
  - Weather backfill behavior supported (`--backfill-weather` / `BACKFILL_WEATHER`).
  - Run metadata + DQ checks are persisted.
- Verify:
  - `curl -sS http://localhost:8090/ops/status`
  - `python3 scripts/smoke_stack.py`

## Deployment (API)

- Implemented in:
  - `services/calculation-service/main.py`
  - `docker-compose.yml`
- Evidence:
  - API routes: `GET /health`, `POST /calculate`, `GET /recommendations/latest`, `GET /recommendations/explain`.
  - Service is containerized and exposed on `localhost:8089`.
- Verify:
  - `curl -sS http://localhost:8089/health`
  - `python3 scripts/smoke_stack.py`

## DQ / Monitoring

- Implemented in:
  - `services/ingestion-worker/ingestion_worker/worker.py`
  - `services/ingestion-worker/ingestion_worker/sources.py`
  - `services/ingestion-worker/ingestion_worker/db.py` (`add_quality_check`)
  - `services/scheduler/scheduler.py` (`/ops/status`)
- Evidence:
  - DQ checks stored in `data_quality_checks`.
  - Scheduler status includes violations for failed runs, stale scheduler activity, and stuck runs.
- Verify:
  - `curl -sS http://localhost:8090/ops/status`
  - Query checks directly in DB (`data_quality_checks` table)

## Versioning / Audit (Reproduce + Explain)

- Implemented in:
  - `services/calculation-service/main.py` (`/recommendations/explain`)
  - `services/model-registry/main.py` (`/models`, `/models/history`)
  - PostgreSQL tables for run metadata and recommendation lineage.
- Evidence:
  - Recommendation records are append-only.
  - Registry keeps registration history.
  - Explain endpoint links recommendation, run metadata, and quality checks.
  - Calculation manifests are emitted with deterministic `decision_snapshot_id` and persisted in `calculation_manifests`.
  - Dedicated replay test verifies deterministic outputs/provenance for identical inputs/version.
- Verify:
  - `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q tests/test_calculation_replay.py`
  - `SMOKE_ENABLE_REPLAY_CHECK=1 python3 scripts/smoke_stack.py`
  - `curl -sS "http://localhost:8089/recommendations/explain?boundary_id=boundary_north_paddock_3&calculation_date=2024-03-15"`
  - `curl -sS "http://localhost:8088/models/history?version_id=v2"`

## Visualization + CI/CD (Design)

- Implemented in:
  - `services/reviewer-ui/Service_Tests.py`
  - `services/reviewer-ui/pages/2_Grazing_Visualization.py`
  - `services/staging-service/main.py`
- Evidence:
  - Reviewer UI includes service test presets and a grazing visualization page.
  - Streamlit `Grazing Visualization` page serves as the rancher-facing mock for assignment review.
  - Staging service requires scheduler ops status to be `ok`, builds the calculation image, runs smoke tests, and writes status to registry.
- Verify:
  - Open `http://localhost:8501`
  - Run staging from UI or:
    - `python3 services/staging-service/main.py --registry-url http://localhost:8088 --service-dir ./services/calculation-service --model-version v2 --image-tag grazeops/calculation-service:staging`

## Current Smoke + Unit Test Commands

- Stack smoke (recommended):
  - `python3 scripts/smoke_stack.py`
- Unit tests:
  - `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q`
