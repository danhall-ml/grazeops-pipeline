#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: int = 10,
) -> tuple[int, dict[str, Any]]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            code = response.getcode()
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        code = exc.code
        body = exc.read().decode("utf-8")
    parsed = json.loads(body) if body else {}
    if not isinstance(parsed, dict):
        raise ValueError(f"non-object JSON returned from {url}")
    return code, parsed


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def check_health(service_name: str, base_url: str, *, timeout_seconds: int) -> None:
    code, payload = request_json(base_url.rstrip("/") + "/health", timeout=timeout_seconds)
    require(code == 200, f"{service_name} /health returned HTTP {code}")
    require(payload.get("status") == "ok", f"{service_name} /health payload was not ok: {payload}")
    print(f"[pass] {service_name} health")


def run_calculation_smoke(
    *,
    calculation_url: str,
    boundary_candidates: list[str],
    calculation_date: str,
    model_version: str,
    config_version: str,
    max_wait_seconds: int,
    retry_seconds: int,
    timeout_seconds: int,
) -> tuple[str, dict[str, Any]]:
    deadline = time.time() + max_wait_seconds
    last_error = "no calculation attempts made"
    lock_errors = 0

    while time.time() <= deadline:
        for boundary_id in boundary_candidates:
            code, payload = request_json(
                calculation_url.rstrip("/") + "/calculate",
                method="POST",
                payload={
                    "boundary_id": boundary_id,
                    "calculation_date": calculation_date,
                    "model_version": model_version,
                    "config_version": config_version,
                },
                timeout=timeout_seconds,
            )
            if code == 200 and payload.get("status") == "ok":
                result = payload.get("result")
                require(isinstance(result, dict), "calculate response missing result object")
                print(f"[pass] calculate ({boundary_id})")
                return boundary_id, result
            error_text = str(payload.get("error") or "")
            if code == 500 and "database is locked" in error_text.lower():
                lock_errors += 1
                if lock_errors >= 3:
                    raise RuntimeError(
                        "calculation failed repeatedly due DB lock contention "
                        "(likely concurrent ingestion run)"
                    )
            last_error = f"{boundary_id}: HTTP {code} {payload}"
        time.sleep(retry_seconds)

    raise RuntimeError(
        "calculation smoke did not succeed before timeout. "
        f"last error: {last_error}"
    )


def assert_deterministic_replay(first: dict[str, Any], second: dict[str, Any]) -> None:
    stable_fields = [
        "boundary_id",
        "calculation_date",
        "model_version",
        "config_version",
        "available_forage_kg",
        "daily_consumption_kg",
        "days_of_grazing_remaining",
        "recommended_move_date",
        "snapshot_id",
    ]
    for field in stable_fields:
        require(
            first.get(field) == second.get(field),
            f"replay mismatch for {field}: first={first.get(field)} second={second.get(field)}",
        )

    first_id = first.get("recommendation_id")
    second_id = second.get("recommendation_id")
    require(isinstance(first_id, int), "first recommendation_id missing or invalid")
    require(isinstance(second_id, int), "second recommendation_id missing or invalid")
    require(first_id != second_id, "replay should create a new append-only recommendation row")
    print("[pass] deterministic replay (stable outputs + append-only write)")


def main() -> None:
    calculation_url = os.getenv("CALCULATION_URL", "http://localhost:8089")
    registry_url = os.getenv("REGISTRY_URL", "http://localhost:8088")
    scheduler_url = os.getenv("SCHEDULER_URL", "http://localhost:8090")
    boundary_candidates = [
        x.strip()
        for x in os.getenv("BOUNDARY_CANDIDATES", "boundary_north_paddock_3").split(",")
        if x.strip()
    ]
    calculation_date = os.getenv("CALCULATION_DATE", "2024-03-15")
    model_version = os.getenv("MODEL_VERSION", "v2")
    config_version = os.getenv("CONFIG_VERSION", "default")
    max_wait_seconds = int(os.getenv("SMOKE_MAX_WAIT_SECONDS", "45"))
    retry_seconds = int(os.getenv("SMOKE_RETRY_SECONDS", "2"))
    timeout_seconds = int(os.getenv("SMOKE_HTTP_TIMEOUT_SECONDS", "45"))
    enable_replay_check = parse_bool(os.getenv("SMOKE_ENABLE_REPLAY_CHECK"), False)

    try:
        check_health("calculation-service", calculation_url, timeout_seconds=timeout_seconds)
        check_health("model-registry", registry_url, timeout_seconds=timeout_seconds)
        check_health("scheduler", scheduler_url, timeout_seconds=timeout_seconds)

        models_code, models_payload = request_json(
            registry_url.rstrip("/") + "/models",
            timeout=timeout_seconds,
        )
        require(models_code == 200, f"model-registry /models returned HTTP {models_code}")
        require(isinstance(models_payload.get("models"), list), "model-registry /models missing models list")
        print("[pass] model registry listing")

        ops_code, ops_payload = request_json(
            scheduler_url.rstrip("/") + "/ops/status",
            timeout=timeout_seconds,
        )
        require(ops_code in {200, 503}, f"scheduler /ops/status returned unexpected HTTP {ops_code}")
        require("status" in ops_payload, "scheduler /ops/status missing status")
        print(f"[pass] scheduler ops status ({ops_payload.get('status')})")

        boundary_id, first_result = run_calculation_smoke(
            calculation_url=calculation_url,
            boundary_candidates=boundary_candidates,
            calculation_date=calculation_date,
            model_version=model_version,
            config_version=config_version,
            max_wait_seconds=max_wait_seconds,
            retry_seconds=retry_seconds,
            timeout_seconds=timeout_seconds,
        )

        recommendation_id: int | None
        result_for_summary = first_result
        if enable_replay_check:
            _, second_result = run_calculation_smoke(
                calculation_url=calculation_url,
                boundary_candidates=[boundary_id],
                calculation_date=calculation_date,
                model_version=model_version,
                config_version=config_version,
                max_wait_seconds=max_wait_seconds,
                retry_seconds=retry_seconds,
                timeout_seconds=timeout_seconds,
            )
            assert_deterministic_replay(first_result, second_result)
            recommendation_id = second_result.get("recommendation_id")
            require(isinstance(recommendation_id, int), "second recommendation_id missing or invalid")
            result_for_summary = second_result
        else:
            recommendation_id = None

        explain_query = (
            f"/recommendations/explain?recommendation_id={recommendation_id}"
            if recommendation_id is not None
            else f"/recommendations/explain?boundary_id={boundary_id}&calculation_date={calculation_date}"
        )
        explain_code, explain_payload = request_json(
            calculation_url.rstrip("/") + explain_query,
            timeout=timeout_seconds,
        )
        require(explain_code == 200, f"/recommendations/explain returned HTTP {explain_code}")
        require(explain_payload.get("status") == "ok", "explain payload status was not ok")
        require(isinstance(explain_payload.get("lineage"), dict), "explain response missing lineage object")
        if recommendation_id is not None:
            explain_reco = explain_payload.get("recommendation")
            require(isinstance(explain_reco, dict), "explain response missing recommendation object")
            require(
                explain_reco.get("recommendation_id") == recommendation_id,
                "explain recommendation_id did not match replay output",
            )
            lineage_versions = explain_payload.get("lineage", {}).get("input_data_versions")
            require(isinstance(lineage_versions, dict), "explain lineage missing input_data_versions")
            require(
                lineage_versions.get("snapshot_id") == result_for_summary.get("snapshot_id"),
                "explain lineage snapshot_id mismatch",
            )
        print("[pass] recommendation explain")

    except Exception as exc:
        print(f"[fail] {exc}")
        sys.exit(1)

    summary = {
        "status": "ok",
        "boundary_id": boundary_id,
        "calculation_date": calculation_date,
        "days_of_grazing_remaining": result_for_summary.get("days_of_grazing_remaining"),
        "recommended_move_date": result_for_summary.get("recommended_move_date"),
        "deterministic_replay": bool(enable_replay_check),
    }
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
