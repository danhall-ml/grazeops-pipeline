---
title: GrazeOps Pipeline
---

## GrazeOps Pipeline

GrazeOps Pipeline is the Part 1 implementation of a grazing recommendation system. The pipeline begins with a ranch boundary and a reporting window, gathers the supporting data needed for that context, and produces a recommendation that can be acted on in operations: how many grazing days remain and when the herd should move.

The repository is organized as a collection of dockerized services. The current architecture is shown below, and grading alignment notes are tracked in `docs/rubric.md`.

![GrazeOps Pipeline Architecture](docs/diagrams/output/grazeops_pipeline_architecture.png)

### Service Responsibilities

- `postgres-db` starts PostgreSQL and loads the base schema before other services start. This keeps setup simple and consistent.
- `ingestion-worker` loads source data for a boundary and date range, writes the prepared data to the DB, and records run status. It also records data checks and weather backfill when gaps exist.
- `scheduler` runs ingestion on a timer and provides `GET /ops/status`. It tells you if runs are failing, stale, or stuck.
- `calculation-service` is the API that runs recommendations and stores results. It also exposes endpoints to fetch the latest result and to explain how a result was produced.
- `model-registry` stores model versions and related metadata. It keeps history so you can see what was registered and when.
- `staging-service` takes a model from the registry, runs smoke checks, and writes pass/fail status back. This gives a clear test step before promotion.
- `reviewer-ui` is the Streamlit app used to test services, view payloads, and inspect outputs and errors in one place.

### Service READMEs

- [`services/postgres-db/README.md`](services/postgres-db/README.md)
- [`services/ingestion-worker/README.md`](services/ingestion-worker/README.md)
- [`services/scheduler/README.md`](services/scheduler/README.md)
- [`services/calculation-service/README.md`](services/calculation-service/README.md)
- [`services/model-registry/README.md`](services/model-registry/README.md)
- [`services/staging-service/README.md`](services/staging-service/README.md)
- [`services/reviewer-ui/README.md`](services/reviewer-ui/README.md)
- [`services/sqlite-db/README.md`](services/sqlite-db/README.md)
- [`services/test-runner/README.md`](services/test-runner/README.md)

## Requirements and Install

For normal usage of this project, install Docker and Docker Compose (v2 plugin). The stack is containerized, so `docker compose up --build` and the recommended ephemeral smoke flow both depend on Docker/Compose.

Host Python is only needed for local host-run scripts. If you run `python3 scripts/smoke_stack.py` directly on your machine, you need Python 3 installed, but no extra libraries for that script (it uses only the standard library).

Unit tests are currently host-run and are not Dockerized in the main service containers. The service images stay runtime-focused and do not include `pytest` or other dev dependencies. If you want to run unit tests, install dev dependencies on host first:

```bash
python3 -m pip install -r requirements-dev.txt
```

If you prefer Dockerized unit tests, use the dedicated test container:

```bash
docker compose --profile test run --rm test-runner
```

## Ingestion (Schedulable + Backfill via Date Ranges)

Ingestion runs through `ingestion-worker` and always stays tied to a boundary context and explicit date range. That keeps normal daily runs and historical backfills on the same execution path instead of maintaining separate tooling. A run accepts a boundary identifier (or boundary GeoJSON) along with start and end dates, and can optionally enable weather backfill when there are missing days inside the selected window.

The implementation uses the provided reference source database (`inputs/pasture_reference.db`) as the default source and can optionally pull live weather from OpenMeteo (`PREFER_OPENMETEO=1`) with fallback to reference weather data. We explicitly handle CRS alignment and daily temporal alignment of RAP/weather records, and any missing or misaligned days are written as data-quality checks.

Scheduling is intentionally handled outside ingestion logic. The `scheduler` service triggers ingestion at a fixed cadence using `SCHEDULE_INTERVAL_SECONDS`, so timing control stays centralized and ingestion code remains focused on data preparation. Each run writes status, timing, and error details to run metadata tables, which gives a clear run history without depending on manual log digging.

## Deployment (API)

Recommendation logic is deployed as `calculation-service`, an HTTP API rather than a local-only script. That makes the same interface available to UI-driven testing, scripted checks, and service-to-service calls.

Operationally, the API keeps a compact shape: `GET /health` for liveness, `POST /calculate` for execution, `GET /recommendations/latest` for current output, and `GET /recommendations/explain` for traceability. Calculation requests persist both recommendation output and run metadata so execution history remains intact.

## DQ/Monitoring (What Checks Exist + Where)

Data quality checks run during ingestion and are stored in the database. The checks target practical reliability concerns: whether expected source data exists in the requested period, whether herd configuration is usable, whether RAP is stale, whether RAP and weather align by day, and whether weather backfill was needed.

Monitoring is intentionally lightweight and focused on actionable status. The scheduler exposes `GET /ops/status`, which summarizes recent failure patterns, stale scheduler activity, and runs that look stuck in progress. This status is also surfaced in reviewer workflows so operational checks happen alongside day-to-day testing instead of in a separate tool.

Alert thresholds and escalation are explicit:

- Stale scheduler activity threshold: `OPS_MAX_TRIGGER_IDLE_SECONDS` (default `max(3 * interval, 900s)`).
- Failed-run threshold: `OPS_MAX_FAILED_RUNS_24H` (default `0`).
- Stuck-run threshold: `OPS_STUCK_RUN_MINUTES` (default `30`).
- If `/ops/status` returns degraded once, open an operations ticket and investigate ingestion run metadata + DQ checks.
- If degraded persists for two scheduler intervals, or if any stuck-run violation appears, page on-call immediately.
- If the issue is source-data freshness/completeness, escalate to DS to confirm threshold overrides vs. data-source outage handling.

## Versioning/Audit (How to Reproduce / Explain)

The pipeline stores run IDs, snapshot IDs, version fields, and timestamps so past recommendations can be reconstructed with context. Ingestion contributes snapshot and run history; calculation contributes recommendation runs and version metadata; together they provide a coherent lineage path.

`GET /recommendations/explain` is the primary audit view. It ties recommendation output back to the source snapshot and run records that produced it, which is the expected answer path for review and grading questions about why a recommendation exists.

## Ownership Boundary

Data Science owns the model logic and parameter choices.

ML Ops owns the system that runs that logic in production: ingestion, scheduling, deployment, monitoring, and run history.

DS ships versioned model/config updates. ML Ops deploys those updates, checks operational health, and rolls back if the update causes failures or degraded monitoring status.

## Viz + CI/CD (Design)

Visualization is handled in the Streamlit app (`services/reviewer-ui/pages/2_Grazing_Visualization.py`) and is used as the rancher-facing mock for this assignment.

For this assignment, the CI/CD flow is intentionally simple:

1. CI validation: run unit tests, run smoke checks, and verify service health endpoints.
2. Release decision: if checks pass, deploy the new version and mark it as staged/promoted in the registry; if checks fail, stop.
3. Recovery path: if the release causes failures, revert to the last known good version and investigate before retrying.

For a real production setup, the same pattern should run with stronger controls:

1. Build and publish: create an immutable image, run dependency/security scans, and publish a versioned artifact.
2. Staging verification: deploy to staging first, run automated integration checks, and compare key metrics to baseline.
3. Progressive rollout: release to production in controlled phases (for example, canary) while watching service and business health.
4. Fast rollback: keep the prior stable version ready so rollback is immediate if errors rise or health degrades.

## Running the System

All assignment inputs are vendored under `./inputs`, so this repository runs without depending on an external sibling repo.

From the repository root, start the stack with:

```bash
docker compose up --build
```

Once services are up, the main entry points are the reviewer UI at `http://localhost:8501`, model registry at `http://localhost:8088`, calculation API at `http://localhost:8089`, and scheduler ops status at `http://localhost:8090/ops/status`.

## Smoke Tests

For this assignment, the recommended smoke path is the isolated ephemeral flow:

```bash
./scripts/smoke_ephemeral.sh
```

This creates a temporary Compose project with its own Postgres volume/network, seeds ingestion once, runs smoke, and tears everything down automatically.

If you already have the stack running and only want a quick check, use:

```bash
python3 scripts/smoke_stack.py
```

If your environment uses different boundary IDs, provide boundary candidates as an override:

```bash
BOUNDARY_CANDIDATES="boundary_paddock_3,boundary_north_paddock_3" python3 scripts/smoke_stack.py
```

You can tune timing behavior with `SMOKE_MAX_WAIT_SECONDS` and `SMOKE_RETRY_SECONDS`, and optionally enable replay stability checks with `SMOKE_ENABLE_REPLAY_CHECK=1`.

## Unit Tests

Unit tests cover ingestion helpers, calculation behavior, and scheduler ops evaluation logic.

Run them from repo root with:

```bash
python3 -m pytest -q
```

Dockerized equivalent:

```bash
docker compose --profile test run --rm test-runner
```

## Design Choices, Real World Deviations

I made certain opinionated design choices during the design of this framework. My intent throughout this project is a basic illustration of some common workflows and patterns.

Sometimes I introduce additional complexity. Smoke tests run in their own ephemeral stack, which is the proper pattern here. This ensures tests do not touch long-running services or shared data. I also switched from the sqlite DB to PostgreSQL to avoid potential concurrency issues with testing while ingestion is running: if a manifest is written to a DB during ingest, this avoids locks.

Elsewhere, I attempt to reduce complexity. The scheduler/orchestrator for running ingestion is really nothing more than a container that runs a cron job. It's clear here that the ingestion engine does not require complex scheduling rules, but in the real world, if you had a more complex orchestrator such as Airflow/Prefect/Dagster running, you might simply hook up the ingestion engine to that instead. Otherwise it would be sufficient to simply use a cloud provider's containerized runtime scheduler, or to quite frankly continue using cron.

For versioning tooling, I used Codex to scaffold a basic model registry service and then wired it into the stack so each run can reference a specific logic version. In reality, I would not build this from scratch—this would typically be handled by MLflow or a managed registry from a cloud vendor—but the lightweight service here makes version history and promotion status explicit for the assignment.

In a more complete setup, the model registry would also be the place where training runs and artifacts are tracked, not just “versions.” That typically means storing run metadata (inputs, parameters, code version, metrics) and pointing to the actual artifacts (serialized models, feature configs, evaluation reports) in durable storage. I don’t implement full training-run tracking or artifact storage in this project because the recommendation logic is simple and the focus is on the deployment pattern, but the registry pattern here is meant to extend naturally in that direction as DS models become more complex.

Were this service to continue running for long periods of time, old data simply needed for audits/reconstruction can be moved out of the database into very cheap tiers of cloud storage. Concretely, the DB only needs to keep the “hot” operational tables (latest runs, recent recommendations, current configs, and enough metadata to locate evidence). Older raw inputs and snapshots can be exported as immutable bundles (one per run with a manifest) into object storage, while the database keeps only pointers (IDs, timestamps, hashes, URIs) so a historical run can be fetched and replayed later without keeping everything online forever.

I do not do typical MLOps drift/deviation monitoring here due to the simplicity of the calculator and the type of data, but I have quite a bit of experience with this specific area. Actual monitoring routines might involve running anomaly detection on incoming data points (I prefer Isolation Forest / Robust Random Cut Forest with heavy compute, simpler statistical calculations for faster scoring), checking for data drift (normalized W1 distance), checking for performance degradation, etc. This often involves setting up more complicated pipelines with queues.

## What I Would Do With More Time

The current system is intentionally compact for the assignment, but there are a few upgrades I would prioritize next. First, I would make the ingestion and calculation contracts stricter by formalizing request/response schemas across services and validating them at each boundary. This would reduce integration mistakes and make failures easier to debug.

Second, I would broaden testing in a practical way. Unit tests are already in place, but I would add a small set of deterministic end-to-end replay cases that run in CI on every pull request and verify full lineage output, not just endpoint success. I would also add one failure-path smoke test that proves rollback and status reporting work as expected when a release fails.

Third, I would harden release operations and make them more automated. The current flow already describes CI/CD clearly, but I would add production-grade deployment safeguards such as environment-specific approval rules, canary progression checks, and an explicit rollback job that can be executed with one command. On the UI side, I would keep the rancher-facing view simple while improving map presentation and recommendation explanations so the tool feels clearer for non-technical users.
