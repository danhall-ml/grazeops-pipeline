# staging-service

## Purpose

This service runs a staging gate for a selected model version: optional ops-gate check, calculation image build/smoke test, and status write-back to model registry.

## API / Inputs / Outputs

- Interface: CLI entrypoint (`python /app/main.py`), one staging run per invocation.
- Inputs:
  - Target model version
  - Registry URL
  - Scheduler URL (for ops gate)
  - Calculation service source directory
- Outputs:
  - Registry update with `staging_status` and `staged_image_tag`
  - JSON result to stdout

## Required Environment Variables

- `MODEL_VERSION`
- `REGISTRY_URL`
- `SERVICE_DIR`

Common optional:

- `SCHEDULER_URL`
- `REQUIRE_OPS_OK`
- `IMAGE_TAG`

## Local Run

From repo root:

```bash
python3 services/staging-service/main.py \
  --registry-url http://localhost:8088 \
  --scheduler-url http://localhost:8090 \
  --service-dir services/calculation-service \
  --model-version v2 \
  --image-tag grazeops/calculation-service:staging
```

## Smoke Check / Health

There is no standalone HTTP health endpoint. Smoke check is a successful staging run exit code (`0`) plus registry update:

```bash
curl -s http://localhost:8088/models | rg "staging_status|version_id"
```

## Dependencies

- Requires Docker daemon access to build/run calculation image.
- Requires `model-registry`.
- If ops gate is enabled, also requires `scheduler /ops/status`.
