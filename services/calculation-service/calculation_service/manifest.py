from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any

from .util import utc_now


def stable_json_dumps(obj: Any) -> str:
    """Serialize object to canonical JSON string.

    Parameters
    ----------
    obj : Any
        JSON-serializable object.

    Returns
    -------
    str
        Stable JSON representation with sorted keys.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    """Compute SHA-256 digest for text input.

    Parameters
    ----------
    value : str
        Input text.

    Returns
    -------
    str
        Hex digest string.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_calculation_manifest(
    *,
    run_id: str,
    boundary_id: str,
    calculation_date: date,
    model_version: str,
    config_version: str,
    ingestion_snapshot_id: str | None,
    rap: dict[str, Any],
    herd: dict[str, Any],
    weather: dict[str, Any],
    utilization_target_pct: float,
    output: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Build deterministic calculation manifest and decision snapshot id.

    Parameters
    ----------
    run_id : str
        Calculation run identifier.
    boundary_id : str
        Boundary identifier.
    calculation_date : date
        Calculation date.
    model_version : str
        Model version used.
    config_version : str
        Config version used.
    ingestion_snapshot_id : str or None
        Snapshot id from ingestion lineage.
    rap : dict[str, Any]
        RAP input payload.
    herd : dict[str, Any]
        Herd input payload.
    weather : dict[str, Any]
        Weather summary payload.
    utilization_target_pct : float
        Effective utilization target used for computation.
    output : dict[str, Any]
        Final recommendation output payload.

    Returns
    -------
    tuple[str, dict[str, Any]]
        ``(decision_snapshot_id, manifest_payload)``.
    """
    idempotency_key = {
        "boundary_id": boundary_id,
        "calculation_date": calculation_date.isoformat(),
        "model_version": model_version,
        "config_version": config_version,
        "herd_config_id": str(herd.get("id") or ""),
    }
    inputs = {
        "ingestion_snapshot_id": ingestion_snapshot_id,
        "rap": {
            "composite_date": rap.get("composite_date"),
            "source_version": rap.get("source_version"),
            "biomass_kg_per_ha": rap.get("biomass_kg_per_ha"),
        },
        "herd": {
            "id": herd.get("id"),
            "animal_count": herd.get("animal_count"),
            "daily_intake_kg_per_head": herd.get("daily_intake_kg_per_head"),
            "utilization_target_pct": utilization_target_pct,
        },
        "weather_summary": {
            "avg_temp_max_7d": weather.get("avg_temp_max_7d"),
            "total_precip_7d": weather.get("total_precip_7d"),
        },
    }
    snapshot_material = {
        "schema_version": 1,
        "idempotency_key": idempotency_key,
        "inputs": inputs,
        "output": output,
    }
    decision_snapshot_id = sha256_text(stable_json_dumps(snapshot_material))
    manifest = {
        "schema_version": 1,
        "decision_snapshot_id": decision_snapshot_id,
        "run_id": run_id,
        "created_at": utc_now(),
        "idempotency_key": idempotency_key,
        "inputs": inputs,
        "output": output,
    }
    return decision_snapshot_id, manifest
