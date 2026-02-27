# ingestion-worker

## Purpose

This service loads source inputs for a boundary and date range, writes prepared records to the operational database, and records ingestion run metadata and data checks.

## API / Inputs / Outputs

- Interface: CLI entrypoint (`python /app/main.py`).
- Inputs:
  - Source reference DB (`--source-db` / `SOURCE_DB_PATH`)
  - Boundary file (`--boundary-path` / `BOUNDARY_PATH`)
  - Herd file (`--herd-path` / `HERD_PATH`)
  - Date window (`--start-date`, `--end-date`)
- Outputs:
  - Boundary, RAP, weather, and herd records in DB tables
  - Ingestion run records and data quality checks
  - Optional run manifest files when `MANIFEST_DIR` is configured

## Required Environment Variables

- `DATABASE_URL` (preferred) or `DB_PATH`
- `SOURCE_DB_PATH`
- `BOUNDARY_PATH`
- `HERD_PATH`
- `START_DATE`
- `END_DATE`

Common optional:

- `BACKFILL_WEATHER`
- `PREFER_OPENMETEO`
- `MANIFEST_DIR`
- `RUN_ID`
- `BOUNDARY_ID`

## Local Run

From repo root:

```bash
python3 services/ingestion-worker/main.py \
  --db-url postgresql://grazeops:grazeops@localhost:5432/grazeops \
  --source-db inputs/pasture_reference.db \
  --boundary-path inputs/sample_boundary.geojson \
  --herd-path inputs/sample_herds_pasturemap.json \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --backfill-weather
```

## Smoke Check / Health

Run a short ingestion window and confirm it exits with code `0`:

```bash
python3 services/ingestion-worker/main.py \
  --db-url postgresql://grazeops:grazeops@localhost:5432/grazeops \
  --source-db inputs/pasture_reference.db \
  --boundary-path inputs/sample_boundary.geojson \
  --herd-path inputs/sample_herds_pasturemap.json \
  --start-date 2024-03-01 \
  --end-date 2024-03-03
```

## Dependencies

- Requires operational DB (`postgres-db` in compose).
- Requires assignment input files under `inputs/`.
