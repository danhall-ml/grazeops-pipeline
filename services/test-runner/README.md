# test-runner

## Purpose

This service provides an isolated container for running the repo unit tests with dev dependencies installed.

## API / Inputs / Outputs

- Interface: `pytest -q` command in container.
- Inputs: repository source mounted at `/workspace`.
- Output: test results to stdout/stderr and process exit code.

## Required Environment Variables

None required for default usage.

## Local Run

From repo root:

```bash
docker compose --profile test run --rm test-runner
```

Host equivalent:

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
```

## Smoke Check / Health

Successful smoke is a clean test run with exit code `0`.

## Dependencies

- Requires Docker for containerized test execution.
