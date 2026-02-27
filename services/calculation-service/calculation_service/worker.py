from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .db import (
    ensure_calculation_manifest_table,
    ensure_calculation_runs_table,
    fetch_boundary,
    fetch_latest_herd_config,
    fetch_latest_rap,
    fetch_weather_summary,
    finish_run_failed,
    finish_run_success,
    latest_snapshot_id_for_date,
    insert_calculation_manifest_if_missing,
    register_model_version,
    resolve_utilization_target_pct,
    start_run,
    upsert_recommendation,
)
from .manifest import build_calculation_manifest
from .models import ModelInputs, calculate_v1, calculate_v2, model_parameters
from .operational_db import connect_operational_db


def export_model_registry_artifact(
    registry_dir: Path,
    *,
    model_version: str,
    config_version: str,
    parameters: dict[str, Any],
) -> Path:
    """Export model metadata artifact for registry/local tracking.

    Parameters
    ----------
    registry_dir : Path
        Output directory for artifact file.
    model_version : str
        Model version identifier.
    config_version : str
        Config version identifier.
    parameters : dict[str, Any]
        Model parameter payload.

    Returns
    -------
    Path
        Path to exported JSON artifact.
    """
    registry_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = registry_dir / f"{model_version}_{config_version}_{ts}.json"
    payload = {
        "model_version": model_version,
        "config_version": config_version,
        "parameters": parameters,
        "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return out_path


def register_model_remote(registry_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Register model metadata in remote registry service.

    Parameters
    ----------
    registry_url : str
        Base URL for model registry service.
    payload : dict[str, Any]
        Registration payload.

    Returns
    -------
    dict[str, Any]
        Parsed registry response payload.
    """
    base = registry_url.rstrip("/")
    url = f"{base}/models/register"
    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Registry response must be a JSON object")
    return parsed


def run_calculation(args: Any) -> dict[str, Any]:
    """Run one end-to-end recommendation calculation.

    Parameters
    ----------
    args : Any
        Runtime arguments namespace built by API layer.

    Returns
    -------
    dict[str, Any]
        Calculation summary including recommendation id and lineage ids.
    """
    conn = connect_operational_db(db_url=args.db_url, db_path=args.db)
    ensure_calculation_runs_table(conn)
    ensure_calculation_manifest_table(conn)

    run_id = args.run_id or (
        f"calc_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_{args.boundary_id}_{args.model_version}"
    )

    start_run(
        conn,
        run_id=run_id,
        scheduled_for=args.scheduled_for,
        boundary_id=args.boundary_id,
        calculation_date=args.calculation_date_obj,
        model_version=args.model_version,
        config_version=args.config_version,
    )

    try:
        boundary = fetch_boundary(conn, args.boundary_id)
        herd = fetch_latest_herd_config(conn, args.boundary_id, args.calculation_date_obj)
        rap = fetch_latest_rap(conn, args.boundary_id, args.calculation_date_obj)
        weather = fetch_weather_summary(conn, args.boundary_id, args.calculation_date_obj)
        utilization_pct = resolve_utilization_target_pct(herd["config_snapshot_json"], args.utilization_target_pct)

        model_inputs = ModelInputs(
            biomass_kg_per_ha=rap["biomass_kg_per_ha"],
            area_ha=boundary["area_ha"],
            animal_count=herd["animal_count"],
            daily_intake_kg_per_head=herd["daily_intake_kg_per_head"],
            utilization_target_pct=utilization_pct,
            avg_temp_max_7d=weather["avg_temp_max_7d"],
            total_precip_7d=weather["total_precip_7d"],
        )

        if args.model_version == "v1":
            output = calculate_v1(args.calculation_date_obj, model_inputs)
        else:
            output = calculate_v2(args.calculation_date_obj, model_inputs)

        if output.days_of_grazing_remaining < 0:
            raise ValueError("invalid recommendation: days_of_grazing_remaining must be >= 0")

        snapshot_id = latest_snapshot_id_for_date(conn, args.boundary_id, args.calculation_date_obj)
        output_payload = {
            "available_forage_kg": output.available_forage_kg,
            "daily_consumption_kg": output.daily_consumption_kg,
            "days_of_grazing_remaining": output.days_of_grazing_remaining,
            "recommended_move_date": output.recommended_move_date.isoformat(),
        }
        decision_snapshot_id, manifest_payload = build_calculation_manifest(
            run_id=run_id,
            boundary_id=args.boundary_id,
            calculation_date=args.calculation_date_obj,
            model_version=args.model_version,
            config_version=args.config_version,
            ingestion_snapshot_id=snapshot_id,
            rap=rap,
            herd=herd,
            weather=weather,
            utilization_target_pct=utilization_pct,
            output=output_payload,
        )

        input_versions = {
            "snapshot_id": snapshot_id,
            "rap_composite_date": rap["composite_date"],
            "rap_source_version": rap["source_version"],
            "herd_config_id": herd["id"],
            "model_version": args.model_version,
            "config_version": args.config_version,
            "decision_snapshot_id": decision_snapshot_id,
        }

        recommendation_id = upsert_recommendation(
            conn,
            boundary_id=args.boundary_id,
            herd_config_id=herd["id"],
            calculation_date=args.calculation_date_obj,
            available_forage_kg=output.available_forage_kg,
            daily_consumption_kg=output.daily_consumption_kg,
            days_of_grazing_remaining=output.days_of_grazing_remaining,
            recommended_move_date=output.recommended_move_date,
            model_version=args.model_version,
            config_version=args.config_version,
            input_data_versions=input_versions,
        )
        insert_calculation_manifest_if_missing(
            conn,
            decision_snapshot_id=decision_snapshot_id,
            recommendation_id=recommendation_id,
            boundary_id=args.boundary_id,
            calculation_date=args.calculation_date_obj,
            model_version=args.model_version,
            config_version=args.config_version,
            manifest=manifest_payload,
        )

        params = model_parameters(args.model_version, utilization_pct)
        registry_export_path = None
        registry_response = None
        if args.register_model:
            register_model_version(
                conn,
                version_id=args.model_version,
                description=f"GrazeOps calculation model {args.model_version}",
                parameters=params,
            )
            if args.registry_dir is not None:
                registry_export_path = export_model_registry_artifact(
                    args.registry_dir,
                    model_version=args.model_version,
                    config_version=args.config_version,
                    parameters=params,
                )
            if args.registry_url:
                registry_payload = {
                    "version_id": args.model_version,
                    "description": f"GrazeOps calculation model {args.model_version}",
                    "parameters": params,
                    "config_version": args.config_version,
                    "artifact_uri": None if registry_export_path is None else str(registry_export_path),
                }
                try:
                    registry_response = register_model_remote(args.registry_url, registry_payload)
                except Exception as exc:
                    registry_response = {
                        "status": "remote_register_failed",
                        "error": str(exc),
                        "registry_url": args.registry_url,
                    }

        finish_run_success(conn, run_id=run_id, recommendation_id=recommendation_id)

        return {
            "run_id": run_id,
            "recommendation_id": recommendation_id,
            "boundary_id": args.boundary_id,
            "calculation_date": args.calculation_date_obj.isoformat(),
            "model_version": args.model_version,
            "config_version": args.config_version,
            "available_forage_kg": output.available_forage_kg,
            "daily_consumption_kg": output.daily_consumption_kg,
            "days_of_grazing_remaining": output.days_of_grazing_remaining,
            "recommended_move_date": output.recommended_move_date.isoformat(),
            "snapshot_id": snapshot_id,
            "decision_snapshot_id": decision_snapshot_id,
            "registry_export": None if registry_export_path is None else str(registry_export_path),
            "registry_response": registry_response,
        }
    except Exception as exc:
        finish_run_failed(conn, run_id=run_id, error=str(exc))
        raise
    finally:
        conn.close()
