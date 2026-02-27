# reviewer-ui

## Purpose

This service is the reviewer-facing Streamlit interface for carrying out the runbook workflows, running service tests, inspecting payloads, and visualizing outputs.

## API / Inputs / Outputs

- Interface: Streamlit app (`Service_Tests.py`) with pages:
  - `Service_Tests.py` (Service Tests)
  - `2_Grazing_Visualization.py`
- Inputs:
  - Service URLs (scheduler, registry, calculation)
  - Default paths and date presets
- Outputs:
  - Test execution results (stdout/stderr and JSON responses)
  - Visualization of grazing recommendation data
  - Operational views used in the runbook: model registration, smoke validation, scheduler status, recent ingestion runs, and failed data-quality checks

## Required Environment Variables

- `WORKSPACE_ROOT`
- `DATABASE_URL` (or `DB_PATH` fallback behavior in helpers)
- `REGISTRY_URL`
- `CALCULATION_URL`
- `SCHEDULER_URL`

Common optional presets:

- `SOURCE_DB_PATH`
- `BOUNDARY_PATH`
- `HERD_PATH`
- `START_DATE`
- `END_DATE`
- `MODEL_VERSION`
- `CONFIG_VERSION`

## Local Run

From this service directory:

```bash
pip install -r requirements.txt
streamlit run Service_Tests.py --server.address=0.0.0.0 --server.port=8501
```

## Smoke Check / Health

```bash
curl -s http://localhost:8501/_stcore/health
```

## Dependencies

- Depends on backend service URLs being reachable for full functionality.
- In compose, mounts repo workspace and input files for test presets.
