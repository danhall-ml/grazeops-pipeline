#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any


def utc_now() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Returns
    -------
    str
        Current time formatted as ``YYYY-mm-ddTHH:MM:SSZ``.
    """
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_bool(value: Any, default: bool = False) -> bool:
    """Parse a flexible truthy/falsey value to boolean.

    Parameters
    ----------
    value : Any
        Candidate boolean input from CLI or environment.
    default : bool, default=False
        Fallback value when parsing is not possible.

    Returns
    -------
    bool
        Parsed boolean result.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def http_json_with_status(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    """Execute HTTP request and return status plus JSON object response.

    Parameters
    ----------
    url : str
        Request URL.
    method : str, default="GET"
        HTTP method.
    payload : dict[str, Any] or None, default=None
        Optional JSON request body.

    Returns
    -------
    tuple[int, dict[str, Any]]
        ``(status_code, parsed_json_object)``.
    """
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = int(resp.getcode())
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        body = exc.read().decode("utf-8")

    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise ValueError("HTTP JSON response must be an object")
    return status, parsed


def http_json(url: str, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Execute HTTP request and raise on non-success status.

    Parameters
    ----------
    url : str
        Request URL.
    method : str, default="GET"
        HTTP method.
    payload : dict[str, Any] or None, default=None
        Optional JSON request body.

    Returns
    -------
    dict[str, Any]
        Parsed JSON response object.
    """
    status, parsed = http_json_with_status(url, method=method, payload=payload)
    if status >= 400:
        raise RuntimeError(f"HTTP {status} error for {url}: {parsed}")
    return parsed


def list_models(registry_url: str) -> list[dict[str, Any]]:
    """List model records from registry service.

    Parameters
    ----------
    registry_url : str
        Registry service base URL.

    Returns
    -------
    list[dict[str, Any]]
        Model records.
    """
    payload = http_json(registry_url.rstrip("/") + "/models")
    models = payload.get("models")
    if not isinstance(models, list):
        raise ValueError("registry response missing models list")
    return [m for m in models if isinstance(m, dict)]


def get_model_by_version(models: list[dict[str, Any]], version_id: str) -> dict[str, Any]:
    """Resolve one model record by version id.

    Parameters
    ----------
    models : list[dict[str, Any]]
        Registry model records.
    version_id : str
        Target model version.

    Returns
    -------
    dict[str, Any]
        Matching model record.

    Raises
    ------
    ValueError
        Raised when version is not found.
    """
    for model in models:
        if str(model.get("version_id") or "") == version_id:
            return model
    raise ValueError(f"model version not found in registry: {version_id}")


def build_and_test_image(service_dir: Path, image_tag: str) -> None:
    """Build calculation service image and run smoke tests inside it.

    Parameters
    ----------
    service_dir : Path
        Calculation service build context path.
    image_tag : str
        Docker image tag to build/test.
    """
    subprocess.run(["docker", "build", "-t", image_tag, str(service_dir)], check=True)
    subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "python", image_tag, "-m", "calculation_service.smoke_tests"],
        check=True,
    )


def assert_ops_gate_ok(scheduler_url: str) -> dict[str, Any]:
    """Assert scheduler ops gate is healthy before staging.

    Parameters
    ----------
    scheduler_url : str
        Scheduler base URL.

    Returns
    -------
    dict[str, Any]
        Parsed ``/ops/status`` payload.
    """
    status_code, payload = http_json_with_status(scheduler_url.rstrip("/") + "/ops/status")
    if status_code not in {200, 503}:
        raise RuntimeError(
            f"staging blocked: scheduler ops endpoint returned HTTP {status_code}: {payload}"
        )

    ops_status = str(payload.get("status") or "")
    if ops_status != "ok":
        violations = payload.get("violations") or []
        raise RuntimeError(
            f"staging blocked: scheduler ops status={ops_status}, violations={violations}"
        )
    return payload


def register_staging_result(
    registry_url: str,
    version_id: str,
    config_version: str,
    base_parameters: dict[str, Any],
    image_tag: str,
) -> dict[str, Any]:
    """Write successful staging status back to model registry.

    Parameters
    ----------
    registry_url : str
        Registry service base URL.
    version_id : str
        Staged model version.
    config_version : str
        Staged config version.
    base_parameters : dict[str, Any]
        Existing model parameters to augment.
    image_tag : str
        Staged image tag.

    Returns
    -------
    dict[str, Any]
        Registry response payload.
    """
    parameters = dict(base_parameters)
    parameters["staging_status"] = "staged"
    parameters["staged_at"] = utc_now()
    parameters["staged_image_tag"] = image_tag

    payload = {
        "version_id": version_id,
        "config_version": config_version,
        "parameters": parameters,
    }
    return http_json(registry_url.rstrip("/") + "/models/register", method="POST", payload=payload)


def parse_args() -> argparse.Namespace:
    """Parse staging-service CLI arguments.

    Returns
    -------
    argparse.Namespace
        Parsed CLI argument namespace.
    """
    parser = argparse.ArgumentParser(description="Task 6 staging service")
    parser.add_argument("--registry-url", default=os.getenv("REGISTRY_URL", "http://model-registry:8080"))
    parser.add_argument(
        "--scheduler-url",
        default=os.getenv("SCHEDULER_URL", "http://scheduler:8082"),
        help="Scheduler base URL used for ops gate checks",
    )
    parser.add_argument(
        "--require-ops-ok",
        default=os.getenv("REQUIRE_OPS_OK", "1"),
        help="If true, staging fails unless scheduler /ops/status is ok",
    )
    parser.add_argument(
        "--service-dir",
        type=Path,
        default=Path(os.getenv("SERVICE_DIR", "/app/services/calculation-service")),
    )
    parser.add_argument(
        "--model-version",
        default=os.getenv("MODEL_VERSION"),
        help="Model version to stage (required)",
    )
    parser.add_argument(
        "--image-tag",
        default=os.getenv("IMAGE_TAG", "grazeops/calculation-service:staging"),
        help="Docker image tag to build and stage",
    )
    args = parser.parse_args()
    if not args.model_version:
        parser.error("--model-version is required (or set MODEL_VERSION)")
    return args


def stage_once(args: argparse.Namespace) -> dict[str, Any]:
    """Execute one staging flow for a model version.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed staging-service arguments.

    Returns
    -------
    dict[str, Any]
        Staging outcome payload.
    """
    ops_payload: dict[str, Any] | None = None
    if parse_bool(args.require_ops_ok, True):
        ops_payload = assert_ops_gate_ok(args.scheduler_url)

    models = list_models(args.registry_url)
    model = get_model_by_version(models, args.model_version)
    config_version = str(model.get("config_version") or "default")
    parameters = model.get("parameters")
    if not isinstance(parameters, dict):
        parameters = {}

    try:
        build_and_test_image(args.service_dir, args.image_tag)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"staging blocked: image build or smoke test failed with exit code {exc.returncode}"
        ) from exc

    registry_result = register_staging_result(
        registry_url=args.registry_url,
        version_id=args.model_version,
        config_version=config_version,
        base_parameters=parameters,
        image_tag=args.image_tag,
    )
    return {
        "status": "staged",
        "version_id": args.model_version,
        "config_version": config_version,
        "image_tag": args.image_tag,
        "registry_result": registry_result,
        "ops_gate": {
            "required": bool(parse_bool(args.require_ops_ok, True)),
            "status": None if ops_payload is None else ops_payload.get("status"),
            "violations": [] if ops_payload is None else ops_payload.get("violations", []),
        },
    }


def main() -> None:
    """Run staging-service entrypoint."""
    args = parse_args()
    result = stage_once(args)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
