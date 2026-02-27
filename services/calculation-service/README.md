# calculation-service

## Purpose

This service is the recommendation API. It runs grazing calculations, stores results, and returns latest/explain views for reviewers.

## API / Inputs / Outputs

- `GET /health`
- `POST /calculate`
- `GET /recommendations/latest?boundary_id=...&calculation_date=...`
- `GET /recommendations/explain?boundary_id=...&calculation_date=...`

`POST /calculate` accepts JSON with fields such as:

- `boundary_id`
- `calculation_date`
- `model_version`
- `config_version`

Outputs:

- Recommendation rows in DB
- Calculation run metadata
- Calculation manifests for explainability

## Required Environment Variables

- `DATABASE_URL` (preferred) or `DB_PATH`
- `REGISTRY_URL`
- `HOST`
- `PORT`

## Local Run

From service directory:

```bash
DATABASE_URL=postgresql://grazeops:grazeops@localhost:5432/grazeops \
REGISTRY_URL=http://localhost:8088 \
python3 main.py
```

## Smoke Check / Health

```bash
curl -s http://localhost:8081/health
```

Example calculation:

```bash
curl -s -X POST http://localhost:8081/calculate \
  -H "Content-Type: application/json" \
  -d '{"boundary_id":"boundary_north_paddock_3","calculation_date":"2024-03-15","model_version":"v2","config_version":"default"}'
```

## Dependencies

- Requires operational DB (`postgres-db` in compose).
- Reads model metadata from `model-registry`.
