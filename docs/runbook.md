# GrazeOps Pipeline Runbook

This runbook covers the three required operational procedures for Part 1:

1. Deploy a model update
2. Investigate a data-quality alert
3. Reproduce a historical recommendation

These procedures can be carried out through the Streamlit reviewer UI at `http://localhost:8501`. For normal reviewer/operator use, the UI is the primary path. The shell commands below are included as the direct backend equivalents for the same workflows and as a lower-level fallback when you want to debug service behavior directly.

- `Operations` tab: register model updates, run smoke validation, inspect scheduler status, inspect recent ingestion runs, inspect recent failed DQ checks
- `Calculation` tab: run calculation checks for a specific version/config
- `Explain` tab: reproduce and inspect historical recommendation lineage

Assumes you are in repo root and core services are up:

```bash
docker compose up -d
```

## 1. Deploy a Model Update

Use this when DS ships a new logic version or parameter update.

UI path: `Service Tests -> Operations`, then `Service Tests -> Calculation`.

This full reviewer workflow is available in the UI. The only shell-only step below is the optional rebuild/restart path if calculation service code itself changed, which is a deployment action rather than a reviewer action.

1. Register or update model metadata in the registry.

```bash
curl -sS -X POST http://localhost:8088/models/register \
  -H 'Content-Type: application/json' \
  -d '{
    "version_id": "v2",
    "config_version": "default",
    "description": "GrazeOps calculation model v2",
    "parameters": {"utilization_target_pct": 50.0}
  }'
```

2. If calculation logic code changed, rebuild and restart the calculation API.

```bash
docker compose build calculation-service
docker compose up -d calculation-service
```

3. Run the model-update validation check (required step in update flow).

```bash
python3 scripts/smoke_stack.py
```

4. Confirm API behavior with an explicit calculate call.

```bash
curl -sS -X POST http://localhost:8089/calculate \
  -H 'Content-Type: application/json' \
  -d '{
    "boundary_id": "boundary_north_paddock_3",
    "calculation_date": "2024-03-15",
    "model_version": "v2",
    "config_version": "default"
  }'
```

5. Rollback if update fails.

- Immediate rollback: route calls back to previous `model_version` (for example `v1`).
- Logic rollback: restore previous code revision, then rebuild/restart `calculation-service` again.

## 2. Investigate a Data-Quality Alert

Use this when scheduler ops status is degraded or the UI indicates data issues.

UI path: `Service Tests -> Operations`.

Everything needed for this investigation is surfaced in the UI: scheduler status, recent ingestion runs, and recent failed DQ checks.

1. Check top-level ops health and violations.

```bash
curl -sS http://localhost:8090/ops/status
```

2. Inspect recent ingestion runs.

```bash
docker compose exec -T postgres-db psql -U grazeops -d grazeops -c "
SELECT ingestion_run_id, status, started_at, ended_at, error, snapshot_id
FROM ingestion_run_metadata
ORDER BY COALESCE(ended_at, started_at) DESC
LIMIT 20;
"
```

3. For a failed run, inspect failing data-quality checks.

```bash
docker compose exec -T postgres-db psql -U grazeops -d grazeops -c "
SELECT run_id, check_name, check_type, passed, details_json, checked_at
FROM data_quality_checks
WHERE run_id = 'REPLACE_WITH_INGESTION_RUN_ID' AND passed = 0
ORDER BY checked_at;
"
```

4. Usual fixes.

- Correct bad boundary/herd input files.
- Re-run ingestion for the affected boundary/date window.
- Confirm RAP/weather availability and staleness thresholds.

## 3. Reproduce a Historical Recommendation

Use this when someone asks why a specific move recommendation was made.

UI path: `Service Tests -> Explain`, then `Service Tests -> Calculation` if you want to re-run the same boundary/date/version inputs.

The full reproduction workflow is available in the UI: fetch the explain payload, capture lineage fields, and re-run the same recommendation inputs.

1. Pull the explain payload for the exact recommendation.

By recommendation id:

```bash
curl -sS "http://localhost:8089/recommendations/explain?recommendation_id=RECOMMENDATION_ID"
```

Or by boundary/date:

```bash
curl -sS "http://localhost:8089/recommendations/explain?boundary_id=boundary_north_paddock_3&calculation_date=2024-03-15"
```

2. Capture these lineage fields from the response.

- `recommendation.model_version`
- `recommendation.config_version`
- `lineage.ingestion_run.snapshot_id`
- `recommendation.calculation_date`
- `recommendation.boundary_id`

3. Re-run with the same boundary/date/version values.

```bash
curl -sS -X POST http://localhost:8089/calculate \
  -H 'Content-Type: application/json' \
  -d '{
    "boundary_id": "boundary_north_paddock_3",
    "calculation_date": "2024-03-15",
    "model_version": "v2",
    "config_version": "default"
  }'
```

4. Compare outputs against the original recommendation.

- `available_forage_kg`
- `daily_consumption_kg`
- `days_of_grazing_remaining`
- `recommended_move_date`

If values differ, inspect the lineage snapshot and ingestion history to identify what changed.
