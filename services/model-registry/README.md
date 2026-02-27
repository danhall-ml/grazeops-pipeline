# model-registry

## Purpose

This service stores model versions and registration history used by calculation and staging flows.

## API / Inputs / Outputs

- `GET /health`
- `GET /models`
- `GET /models/history`
- `POST /models/register`

Register payload example fields:

- `version_id`
- `config_version`
- `parameters` (object)
- `description` (optional)

Output is persisted in `models.json` under the configured registry directory.

## Required Environment Variables

- `HOST` (default `0.0.0.0`)
- `PORT` (default `8080`)
- `REGISTRY_DIR` (default `/registry-data`)

## Local Run

From service directory:

```bash
REGISTRY_DIR=../../model-registry python3 main.py
```

## Smoke Check / Health

```bash
curl -s http://localhost:8080/health
curl -s -X POST http://localhost:8080/models/register \
  -H "Content-Type: application/json" \
  -d '{"version_id":"v2","config_version":"default","parameters":{"logic":"baseline"}}'
curl -s http://localhost:8080/models
```

## Dependencies

- No upstream runtime dependency.
- In compose, `./model-registry` is mounted for persistence.
