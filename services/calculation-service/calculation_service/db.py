from __future__ import annotations

import json
from datetime import date
from typing import Any

from .util import utc_now


def ensure_calculation_runs_table(conn: Any) -> None:
    """Ensure calculation run metadata table exists.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS calculation_runs (
            run_id TEXT PRIMARY KEY,
            scheduled_for TEXT,
            boundary_id TEXT NOT NULL,
            calculation_date TEXT NOT NULL,
            model_version TEXT NOT NULL,
            config_version TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            recommendation_id INTEGER,
            error TEXT
        )
        """
    )


def ensure_calculation_manifest_table(conn: Any) -> None:
    """Ensure calculation manifest table and index exist.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS calculation_manifests (
            decision_snapshot_id TEXT PRIMARY KEY,
            recommendation_id INTEGER NOT NULL,
            boundary_id TEXT NOT NULL,
            calculation_date TEXT NOT NULL,
            model_version TEXT NOT NULL,
            config_version TEXT,
            manifest_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_calculation_manifests_recommendation_id
        ON calculation_manifests(recommendation_id)
        """
    )


def fetch_boundary(conn: Any, boundary_id: str) -> dict[str, Any]:
    """Fetch boundary area for calculation.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    boundary_id : str
        Boundary identifier.

    Returns
    -------
    dict[str, Any]
        Boundary payload with validated ``area_ha``.

    Raises
    ------
    ValueError
        Raised when boundary is missing or has invalid area.
    """
    row = conn.execute(
        """
        SELECT area_ha
        FROM geographic_boundaries
        WHERE boundary_id = ?
        LIMIT 1
        """,
        (boundary_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Boundary not found: {boundary_id}")
    if row["area_ha"] is None or float(row["area_ha"]) <= 0:
        raise ValueError(f"Boundary area_ha invalid for: {boundary_id}")
    return {
        "area_ha": float(row["area_ha"]),
    }


def fetch_latest_herd_config(conn: Any, boundary_id: str, as_of: date) -> dict[str, Any]:
    """Fetch active or latest herd configuration for a boundary/date.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    boundary_id : str
        Boundary identifier.
    as_of : date
        Calculation date used for valid-from/valid-to filtering.

    Returns
    -------
    dict[str, Any]
        Herd configuration payload for calculation.

    Raises
    ------
    ValueError
        Raised when no herd configuration exists for boundary.
    """
    row = conn.execute(
        """
        SELECT id, animal_count, daily_intake_kg_per_head, config_snapshot_json
        FROM herd_configurations
        WHERE boundary_id = ?
          AND valid_from <= ?
          AND (valid_to IS NULL OR valid_to >= ?)
        ORDER BY valid_from DESC
        LIMIT 1
        """,
        (boundary_id, as_of.isoformat(), as_of.isoformat()),
    ).fetchone()

    if row is None:
        row = conn.execute(
            """
            SELECT id, animal_count, daily_intake_kg_per_head, config_snapshot_json
            FROM herd_configurations
            WHERE boundary_id = ?
            ORDER BY valid_from DESC
            LIMIT 1
            """,
            (boundary_id,),
        ).fetchone()

    if row is None:
        raise ValueError(f"No herd configuration for boundary: {boundary_id}")

    return {
        "id": str(row["id"]),
        "animal_count": int(row["animal_count"]),
        "daily_intake_kg_per_head": float(row["daily_intake_kg_per_head"]),
        "config_snapshot_json": str(row["config_snapshot_json"] or "{}"),
    }


def fetch_latest_rap(conn: Any, boundary_id: str, as_of: date) -> dict[str, Any]:
    """Fetch latest RAP biomass composite at or before as-of date.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    boundary_id : str
        Boundary identifier.
    as_of : date
        Calculation date upper bound.

    Returns
    -------
    dict[str, Any]
        RAP payload used by calculation.

    Raises
    ------
    ValueError
        Raised when no RAP rows exist up to as-of date.
    """
    row = conn.execute(
        """
        SELECT composite_date, biomass_kg_per_ha, source_version
        FROM rap_biomass
        WHERE boundary_id = ? AND composite_date <= ?
        ORDER BY composite_date DESC
        LIMIT 1
        """,
        (boundary_id, as_of.isoformat()),
    ).fetchone()
    if row is None:
        raise ValueError(f"No RAP biomass for boundary {boundary_id} up to {as_of.isoformat()}")
    return {
        "composite_date": str(row["composite_date"]),
        "biomass_kg_per_ha": float(row["biomass_kg_per_ha"]),
        "source_version": str(row["source_version"] or "reference_db"),
    }


def fetch_weather_summary(conn: Any, boundary_id: str, as_of: date) -> dict[str, float | None]:
    """Compute seven-day weather summary at calculation date.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    boundary_id : str
        Boundary identifier.
    as_of : date
        Calculation date upper bound.

    Returns
    -------
    dict[str, float | None]
        Weather summary with average max temperature and total precipitation.
    """
    row = conn.execute(
        """
        SELECT
            AVG(temp_max_c) AS avg_temp_max_7d,
            SUM(COALESCE(precipitation_mm, 0.0)) AS total_precip_7d
        FROM (
            SELECT temp_max_c, precipitation_mm
            FROM weather_forecasts
            WHERE boundary_id = ? AND forecast_date <= ?
            ORDER BY forecast_date DESC
            LIMIT 7
        ) AS recent_weather
        """,
        (boundary_id, as_of.isoformat()),
    ).fetchone()
    if row is None:
        return {"avg_temp_max_7d": None, "total_precip_7d": None}
    return {
        "avg_temp_max_7d": None if row["avg_temp_max_7d"] is None else float(row["avg_temp_max_7d"]),
        "total_precip_7d": None
        if row["total_precip_7d"] is None
        else float(row["total_precip_7d"]),
    }


def resolve_utilization_target_pct(herd_config_snapshot_json: str, override: float | None) -> float:
    """Resolve utilization target from explicit override or herd snapshot.

    Parameters
    ----------
    herd_config_snapshot_json : str
        Herd config snapshot JSON text.
    override : float or None
        Explicit utilization target override.

    Returns
    -------
    float
        Effective utilization target percentage.
    """
    if override is not None:
        return float(override)
    try:
        payload = json.loads(herd_config_snapshot_json)
    except Exception:
        payload = {}
    herd = payload.get("herd") if isinstance(payload, dict) else None
    if isinstance(herd, dict) and isinstance(herd.get("utilization_target_pct"), (int, float)):
        value = float(herd["utilization_target_pct"])
        if value > 0:
            return value
    return 50.0


def latest_snapshot_id_for_date(conn: Any, boundary_id: str, as_of: date) -> str | None:
    """Fetch latest successful ingestion snapshot covering as-of date.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    boundary_id : str
        Boundary identifier.
    as_of : date
        Target calculation date.

    Returns
    -------
    str or None
        Snapshot id when available, otherwise ``None``.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_run_metadata (
            ingestion_run_id TEXT PRIMARY KEY,
            scheduled_for TEXT,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,
            error TEXT,
            snapshot_id TEXT NOT NULL
        )
        """
    )
    row = conn.execute(
        """
        SELECT m.snapshot_id
        FROM ingestion_run_metadata m
        JOIN ingestion_runs r ON r.run_id = m.ingestion_run_id
        WHERE r.boundary_id = ?
          AND r.status = 'completed'
          AND m.status = 'success'
          AND r.timeframe_start <= ?
          AND r.timeframe_end >= ?
        ORDER BY COALESCE(m.ended_at, r.completed_at, r.started_at) DESC
        LIMIT 1
        """,
        (boundary_id, as_of.isoformat(), as_of.isoformat()),
    ).fetchone()
    return None if row is None else str(row["snapshot_id"])


def start_run(
    conn: Any,
    run_id: str,
    scheduled_for: str | None,
    boundary_id: str,
    calculation_date: date,
    model_version: str,
    config_version: str,
) -> None:
    """Insert initial calculation run record.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    run_id : str
        Calculation run identifier.
    scheduled_for : str or None
        Scheduler timestamp for run trigger.
    boundary_id : str
        Boundary identifier.
    calculation_date : date
        Calculation date.
    model_version : str
        Model version.
    config_version : str
        Config version.
    """
    conn.execute(
        """
        INSERT INTO calculation_runs (
            run_id, scheduled_for, boundary_id, calculation_date, model_version, config_version,
            status, started_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'running', ?)
        """,
        (
            run_id,
            scheduled_for,
            boundary_id,
            calculation_date.isoformat(),
            model_version,
            config_version,
            utc_now(),
        ),
    )
    conn.commit()


def finish_run_success(conn: Any, run_id: str, recommendation_id: int) -> None:
    """Mark calculation run as successful.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    run_id : str
        Calculation run identifier.
    recommendation_id : int
        Produced recommendation identifier.
    """
    conn.execute(
        """
        UPDATE calculation_runs
        SET status='success', ended_at=?, recommendation_id=?, error=NULL
        WHERE run_id=?
        """,
        (utc_now(), recommendation_id, run_id),
    )
    conn.commit()


def finish_run_failed(conn: Any, run_id: str, error: str) -> None:
    """Mark calculation run as failed.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    run_id : str
        Calculation run identifier.
    error : str
        Failure message.
    """
    conn.execute(
        """
        UPDATE calculation_runs
        SET status='failed', ended_at=?, error=?
        WHERE run_id=?
        """,
        (utc_now(), error, run_id),
    )
    conn.commit()


def upsert_recommendation(
    conn: Any,
    *,
    boundary_id: str,
    herd_config_id: str,
    calculation_date: date,
    available_forage_kg: float,
    daily_consumption_kg: float,
    days_of_grazing_remaining: float,
    recommended_move_date: date,
    model_version: str,
    config_version: str,
    input_data_versions: dict[str, Any],
) -> int:
    """Insert one grazing recommendation row.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    boundary_id : str
        Boundary identifier.
    herd_config_id : str
        Herd configuration identifier.
    calculation_date : date
        Calculation date.
    available_forage_kg : float
        Calculated available forage.
    daily_consumption_kg : float
        Calculated daily herd consumption.
    days_of_grazing_remaining : float
        Calculated grazing days remaining.
    recommended_move_date : date
        Recommended herd move date.
    model_version : str
        Model version.
    config_version : str
        Config version.
    input_data_versions : dict[str, Any]
        Input lineage/version payload serialized in recommendation row.

    Returns
    -------
    int
        Inserted recommendation identifier.

    Raises
    ------
    RuntimeError
        Raised when insert does not return a new id.
    """
    cur = conn.execute(
        """
        INSERT INTO grazing_recommendations (
            boundary_id, herd_config_id, calculation_date, available_forage_kg,
            daily_consumption_kg, days_of_grazing_remaining, recommended_move_date,
            model_version, config_version, input_data_versions_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id
        """,
        (
            boundary_id,
            herd_config_id,
            calculation_date.isoformat(),
            round(available_forage_kg, 3),
            round(daily_consumption_kg, 3),
            round(days_of_grazing_remaining, 3),
            recommended_move_date.isoformat(),
            model_version,
            config_version,
            json.dumps(input_data_versions, sort_keys=True),
            utc_now(),
        ),
    )
    row = cur.fetchone()
    conn.commit()
    if row is None:
        raise RuntimeError("failed to insert recommendation")
    return int(row["id"])


def register_model_version(
    conn: Any,
    version_id: str,
    description: str,
    parameters: dict[str, Any],
) -> None:
    """Register model version metadata if missing.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    version_id : str
        Model version identifier.
    description : str
        Human-readable description.
    parameters : dict[str, Any]
        Model parameter metadata.
    """
    now = utc_now()
    conn.execute(
        """
        INSERT INTO model_versions (
            version_id, description, parameters_json, deployed_at, deprecated_at, created_at
        ) VALUES (?, ?, ?, ?, NULL, ?)
        ON CONFLICT(version_id) DO NOTHING
        """,
        (version_id, description, json.dumps(parameters, sort_keys=True), now, now),
    )
    conn.commit()


def insert_calculation_manifest_if_missing(
    conn: Any,
    *,
    decision_snapshot_id: str,
    recommendation_id: int,
    boundary_id: str,
    calculation_date: date,
    model_version: str,
    config_version: str,
    manifest: dict[str, Any],
) -> None:
    """Insert calculation manifest record if not already present.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    decision_snapshot_id : str
        Deterministic decision snapshot identifier.
    recommendation_id : int
        Recommendation identifier.
    boundary_id : str
        Boundary identifier.
    calculation_date : date
        Calculation date.
    model_version : str
        Model version.
    config_version : str
        Config version.
    manifest : dict[str, Any]
        Full calculation manifest payload.
    """
    conn.execute(
        """
        INSERT INTO calculation_manifests (
            decision_snapshot_id, recommendation_id, boundary_id, calculation_date,
            model_version, config_version, manifest_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(decision_snapshot_id) DO NOTHING
        """,
        (
            decision_snapshot_id,
            recommendation_id,
            boundary_id,
            calculation_date.isoformat(),
            model_version,
            config_version,
            json.dumps(manifest, sort_keys=True),
            utc_now(),
        ),
    )
    conn.commit()
