#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlparse

from calculation_service.operational_db import connect_operational_db
from calculation_service.util import parse_date
from calculation_service.worker import run_calculation


VALID_MODELS = {"v1", "v2"}


def utc_now() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Returns
    -------
    str
        Current time formatted as ``YYYY-mm-ddTHH:MM:SSZ``.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_bool(value: Any, default: bool = False) -> bool:
    """Parse a flexible truthy/falsey value to boolean.

    Parameters
    ----------
    value : Any
        Candidate boolean input from payload or environment.
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


def build_run_args(payload: dict[str, Any]) -> SimpleNamespace:
    """Build and validate calculation runtime arguments from request payload.

    Parameters
    ----------
    payload : dict[str, Any]
        JSON request body for ``POST /calculate``.

    Returns
    -------
    SimpleNamespace
        Normalized runtime arguments consumed by ``run_calculation``.
    """
    boundary_id = str(payload.get("boundary_id") or "").strip()
    if not boundary_id:
        raise ValueError("boundary_id is required")

    calculation_date = str(payload.get("calculation_date") or "").strip()
    if not calculation_date:
        raise ValueError("calculation_date is required")
    calculation_date_obj = parse_date(calculation_date)

    model_version = str(payload.get("model_version") or os.getenv("MODEL_VERSION", "v1")).strip().lower()
    if model_version not in VALID_MODELS:
        raise ValueError(f"model_version must be one of: {sorted(VALID_MODELS)}")

    util_raw = payload.get("utilization_target_pct")
    utilization_target_pct: float | None
    if util_raw in (None, ""):
        utilization_target_pct = None
    else:
        utilization_target_pct = float(util_raw)
        if not (0 < utilization_target_pct <= 100):
            raise ValueError("utilization_target_pct must be > 0 and <= 100")

    db_url_raw = payload.get("db_url") or os.getenv("DATABASE_URL")
    db_url = None if db_url_raw in (None, "") else str(db_url_raw)
    db_raw = payload.get("db") or os.getenv("DB_PATH", "/data/grazeops.db")
    db_path = Path(str(db_raw)) if db_raw not in (None, "") else None

    register_model = parse_bool(payload.get("register_model"), parse_bool(os.getenv("REGISTER_MODEL"), False))
    registry_dir_raw = payload.get("registry_dir") or os.getenv("REGISTRY_DIR")
    registry_url = payload.get("registry_url") or os.getenv("REGISTRY_URL")

    run_id_raw = payload.get("run_id")
    scheduled_for_raw = payload.get("scheduled_for")

    run_id = None if run_id_raw in (None, "") else str(run_id_raw)
    scheduled_for = None if scheduled_for_raw in (None, "") else str(scheduled_for_raw)

    return SimpleNamespace(
        db=db_path,
        db_url=db_url,
        boundary_id=boundary_id,
        calculation_date=calculation_date,
        calculation_date_obj=calculation_date_obj,
        model_version=model_version,
        config_version=str(payload.get("config_version") or os.getenv("CONFIG_VERSION", "default")),
        utilization_target_pct=utilization_target_pct,
        run_id=run_id,
        scheduled_for=scheduled_for,
        register_model=register_model,
        registry_dir=None if registry_dir_raw in (None, "") else Path(str(registry_dir_raw)),
        registry_url=None if registry_url in (None, "") else str(registry_url),
    )


def fetch_latest_recommendation(
    db_url: str | None,
    db_path: Path | None,
    boundary_id: str,
    calculation_date: str | None,
) -> dict[str, Any] | None:
    """Fetch the latest recommendation for a boundary/date query.

    Parameters
    ----------
    db_url : str or None
        PostgreSQL URL. When provided, preferred over ``db_path``.
    db_path : Path or None
        SQLite path fallback when ``db_url`` is not set.
    boundary_id : str
        Boundary identifier.
    calculation_date : str or None
        Optional calculation date filter (``YYYY-mm-dd``).

    Returns
    -------
    dict[str, Any] or None
        Recommendation payload, or ``None`` when no row matches.
    """
    conn = connect_operational_db(db_url=db_url, db_path=db_path)
    try:
        if calculation_date:
            row = conn.execute(
                """
                SELECT id, boundary_id, calculation_date, available_forage_kg, daily_consumption_kg,
                       days_of_grazing_remaining, recommended_move_date, model_version, config_version,
                       input_data_versions_json, created_at
                FROM grazing_recommendations
                WHERE boundary_id = ? AND calculation_date = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (boundary_id, calculation_date),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id, boundary_id, calculation_date, available_forage_kg, daily_consumption_kg,
                       days_of_grazing_remaining, recommended_move_date, model_version, config_version,
                       input_data_versions_json, created_at
                FROM grazing_recommendations
                WHERE boundary_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (boundary_id,),
            ).fetchone()
        if row is None:
            return None

        versions: dict[str, Any] = {}
        raw_versions = row["input_data_versions_json"]
        if raw_versions:
            try:
                parsed = json.loads(str(raw_versions))
                if isinstance(parsed, dict):
                    versions = parsed
            except Exception:
                versions = {}

        return {
            "recommendation_id": int(row["id"]),
            "boundary_id": str(row["boundary_id"]),
            "calculation_date": str(row["calculation_date"]),
            "available_forage_kg": float(row["available_forage_kg"]),
            "daily_consumption_kg": float(row["daily_consumption_kg"]),
            "days_of_grazing_remaining": float(row["days_of_grazing_remaining"]),
            "recommended_move_date": str(row["recommended_move_date"]),
            "model_version": str(row["model_version"]),
            "config_version": str(row["config_version"] or ""),
            "input_data_versions": versions,
            "created_at": str(row["created_at"]),
        }
    finally:
        conn.close()


def parse_json_dict(raw: Any) -> dict[str, Any]:
    """Parse an arbitrary value as JSON object.

    Parameters
    ----------
    raw : Any
        Raw JSON-like value.

    Returns
    -------
    dict[str, Any]
        Parsed dictionary, or an empty dictionary on failure.
    """
    if raw in (None, ""):
        return {}
    try:
        parsed = json.loads(str(raw))
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {}


def query_one(conn: Any, sql: str, params: tuple[Any, ...]) -> Any | None:
    """Execute a query and return one row, swallowing query errors.

    Parameters
    ----------
    conn : Any
        Operational database connection.
    sql : str
        SQL statement.
    params : tuple[Any, ...]
        SQL parameters.

    Returns
    -------
    Any or None
        Single row when query succeeds, otherwise ``None``.
    """
    try:
        return conn.execute(sql, params).fetchone()
    except Exception:
        return None


def query_all(conn: Any, sql: str, params: tuple[Any, ...]) -> list[Any]:
    """Execute a query and return all rows, swallowing query errors.

    Parameters
    ----------
    conn : Any
        Operational database connection.
    sql : str
        SQL statement.
    params : tuple[Any, ...]
        SQL parameters.

    Returns
    -------
    list[Any]
        Result rows, or an empty list on failure.
    """
    try:
        return conn.execute(sql, params).fetchall()
    except Exception:
        return []


def fetch_recommendation_explain(
    db_url: str | None,
    db_path: Path | None,
    *,
    boundary_id: str,
    calculation_date: str | None,
    recommendation_id: int | None,
) -> dict[str, Any] | None:
    """Build lineage and explain payload for a recommendation.

    Parameters
    ----------
    db_url : str or None
        PostgreSQL URL. When provided, preferred over ``db_path``.
    db_path : Path or None
        SQLite path fallback when ``db_url`` is not set.
    boundary_id : str
        Boundary identifier used for latest lookup when ``recommendation_id`` is omitted.
    calculation_date : str or None
        Optional date filter used with ``boundary_id``.
    recommendation_id : int or None
        Explicit recommendation identifier.

    Returns
    -------
    dict[str, Any] or None
        Explain payload containing recommendation + lineage, or ``None`` when not found.
    """
    conn = connect_operational_db(db_url=db_url, db_path=db_path)
    try:
        rec_row: Any | None
        if recommendation_id is not None:
            rec_row = query_one(
                conn,
                """
                SELECT id, boundary_id, herd_config_id, calculation_date, available_forage_kg, daily_consumption_kg,
                       days_of_grazing_remaining, recommended_move_date, model_version, config_version,
                       input_data_versions_json, created_at
                FROM grazing_recommendations
                WHERE id = ?
                LIMIT 1
                """,
                (recommendation_id,),
            )
        elif calculation_date:
            rec_row = query_one(
                conn,
                """
                SELECT id, boundary_id, herd_config_id, calculation_date, available_forage_kg, daily_consumption_kg,
                       days_of_grazing_remaining, recommended_move_date, model_version, config_version,
                       input_data_versions_json, created_at
                FROM grazing_recommendations
                WHERE boundary_id = ? AND calculation_date = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (boundary_id, calculation_date),
            )
        else:
            rec_row = query_one(
                conn,
                """
                SELECT id, boundary_id, herd_config_id, calculation_date, available_forage_kg, daily_consumption_kg,
                       days_of_grazing_remaining, recommended_move_date, model_version, config_version,
                       input_data_versions_json, created_at
                FROM grazing_recommendations
                WHERE boundary_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (boundary_id,),
            )

        if rec_row is None:
            return None

        rec_boundary_id = str(rec_row["boundary_id"])
        rec_calc_date = str(rec_row["calculation_date"])
        rec_model_version = str(rec_row["model_version"])
        rec_config_version = str(rec_row["config_version"] or "")
        rec_herd_config_id = str(rec_row["herd_config_id"])
        input_versions = parse_json_dict(rec_row["input_data_versions_json"])
        snapshot_id = str(input_versions.get("snapshot_id") or "")
        decision_snapshot_id = str(input_versions.get("decision_snapshot_id") or "")

        boundary_row = query_one(
            conn,
            """
            SELECT boundary_id, name, ranch_id, pasture_id, area_ha, crs, source_file
            FROM geographic_boundaries
            WHERE boundary_id = ?
            LIMIT 1
            """,
            (rec_boundary_id,),
        )

        herd_row = query_one(
            conn,
            """
            SELECT id, ranch_id, pasture_id, animal_count, animal_type, daily_intake_kg_per_head,
                   avg_daily_gain_kg, valid_from, valid_to, config_snapshot_json, created_at
            FROM herd_configurations
            WHERE id = ?
            LIMIT 1
            """,
            (rec_herd_config_id,),
        )

        model_row = query_one(
            conn,
            """
            SELECT version_id, description, parameters_json, deployed_at, deprecated_at, created_at
            FROM model_versions
            WHERE version_id = ?
            LIMIT 1
            """,
            (rec_model_version,),
        )

        calc_run_row = query_one(
            conn,
            """
            SELECT run_id, scheduled_for, boundary_id, calculation_date, model_version, config_version,
                   status, started_at, ended_at, recommendation_id, error
            FROM calculation_runs
            WHERE recommendation_id = ?
            ORDER BY COALESCE(ended_at, started_at) DESC
            LIMIT 1
            """,
            (int(rec_row["id"]),),
        )

        ingestion_row: Any | None = None
        if snapshot_id:
            ingestion_row = query_one(
                conn,
                """
                SELECT m.ingestion_run_id, m.scheduled_for, m.started_at, m.ended_at, m.status, m.error, m.snapshot_id,
                       r.boundary_id, r.timeframe_start, r.timeframe_end, r.sources_included, r.records_ingested
                FROM ingestion_run_metadata m
                LEFT JOIN ingestion_runs r ON r.run_id = m.ingestion_run_id
                WHERE m.snapshot_id = ?
                ORDER BY COALESCE(m.ended_at, m.started_at) DESC
                LIMIT 1
                """,
                (snapshot_id,),
            )

        quality_rows: list[Any] = []
        if ingestion_row is not None and ingestion_row["ingestion_run_id"] is not None:
            quality_rows = query_all(
                conn,
                """
                SELECT check_name, check_type, passed, details_json, checked_at
                FROM data_quality_checks
                WHERE run_id = ?
                ORDER BY checked_at ASC
                """,
                (str(ingestion_row["ingestion_run_id"]),),
            )

        calculation_manifest_row: Any | None = None
        if decision_snapshot_id:
            calculation_manifest_row = query_one(
                conn,
                """
                SELECT decision_snapshot_id, recommendation_id, boundary_id, calculation_date,
                       model_version, config_version, manifest_json, created_at
                FROM calculation_manifests
                WHERE decision_snapshot_id = ?
                LIMIT 1
                """,
                (decision_snapshot_id,),
            )

        recommendation = {
            "recommendation_id": int(rec_row["id"]),
            "boundary_id": rec_boundary_id,
            "herd_config_id": rec_herd_config_id,
            "calculation_date": rec_calc_date,
            "recommended_move_date": str(rec_row["recommended_move_date"]),
            "days_of_grazing_remaining": float(rec_row["days_of_grazing_remaining"]),
            "available_forage_kg": float(rec_row["available_forage_kg"]),
            "daily_consumption_kg": float(rec_row["daily_consumption_kg"]),
            "model_version": rec_model_version,
            "config_version": rec_config_version,
            "created_at": str(rec_row["created_at"]),
        }

        boundary = None
        if boundary_row is not None:
            boundary = {
                "boundary_id": str(boundary_row["boundary_id"]),
                "name": str(boundary_row["name"]),
                "ranch_id": boundary_row["ranch_id"],
                "pasture_id": boundary_row["pasture_id"],
                "area_ha": None if boundary_row["area_ha"] is None else float(boundary_row["area_ha"]),
                "crs": boundary_row["crs"],
                "source_file": boundary_row["source_file"],
            }

        herd_configuration = None
        if herd_row is not None:
            herd_configuration = {
                "id": str(herd_row["id"]),
                "ranch_id": herd_row["ranch_id"],
                "pasture_id": herd_row["pasture_id"],
                "animal_count": int(herd_row["animal_count"]),
                "animal_type": herd_row["animal_type"],
                "daily_intake_kg_per_head": float(herd_row["daily_intake_kg_per_head"]),
                "avg_daily_gain_kg": None
                if herd_row["avg_daily_gain_kg"] is None
                else float(herd_row["avg_daily_gain_kg"]),
                "valid_from": herd_row["valid_from"],
                "valid_to": herd_row["valid_to"],
                "created_at": herd_row["created_at"],
            }

        model = {
            "model_version": rec_model_version,
            "config_version": rec_config_version,
            "parameters": None,
            "description": None,
            "deployed_at": None,
            "deprecated_at": None,
            "created_at": None,
        }
        if model_row is not None:
            model["parameters"] = parse_json_dict(model_row["parameters_json"])
            model["description"] = model_row["description"]
            model["deployed_at"] = model_row["deployed_at"]
            model["deprecated_at"] = model_row["deprecated_at"]
            model["created_at"] = model_row["created_at"]

        calculation_run = None
        if calc_run_row is not None:
            calculation_run = {
                "run_id": calc_run_row["run_id"],
                "scheduled_for": calc_run_row["scheduled_for"],
                "status": calc_run_row["status"],
                "started_at": calc_run_row["started_at"],
                "ended_at": calc_run_row["ended_at"],
                "error": calc_run_row["error"],
            }

        ingestion_run = None
        if ingestion_row is not None:
            ingestion_run = {
                "ingestion_run_id": ingestion_row["ingestion_run_id"],
                "snapshot_id": ingestion_row["snapshot_id"],
                "scheduled_for": ingestion_row["scheduled_for"],
                "started_at": ingestion_row["started_at"],
                "ended_at": ingestion_row["ended_at"],
                "status": ingestion_row["status"],
                "error": ingestion_row["error"],
                "boundary_id": ingestion_row["boundary_id"],
                "timeframe_start": ingestion_row["timeframe_start"],
                "timeframe_end": ingestion_row["timeframe_end"],
                "sources_included": ingestion_row["sources_included"],
                "records_ingested": ingestion_row["records_ingested"],
            }

        quality_checks: list[dict[str, Any]] = []
        for row in quality_rows:
            quality_checks.append(
                {
                    "check_name": str(row["check_name"]),
                    "check_type": row["check_type"],
                    "passed": bool(int(row["passed"])),
                    "details": parse_json_dict(row["details_json"]),
                    "checked_at": row["checked_at"],
                }
            )

        calculation_manifest = None
        if calculation_manifest_row is not None:
            calculation_manifest = {
                "decision_snapshot_id": str(calculation_manifest_row["decision_snapshot_id"]),
                "recommendation_id": int(calculation_manifest_row["recommendation_id"]),
                "boundary_id": str(calculation_manifest_row["boundary_id"]),
                "calculation_date": str(calculation_manifest_row["calculation_date"]),
                "model_version": str(calculation_manifest_row["model_version"]),
                "config_version": str(calculation_manifest_row["config_version"] or ""),
                "created_at": str(calculation_manifest_row["created_at"]),
                "manifest": parse_json_dict(calculation_manifest_row["manifest_json"]),
            }

        return {
            "recommendation": recommendation,
            "lineage": {
                "boundary": boundary,
                "herd_configuration": herd_configuration,
                "model": model,
                "input_data_versions": input_versions,
                "calculation_run": calculation_run,
                "ingestion_run": ingestion_run,
                "quality_checks": quality_checks,
                "calculation_manifest": calculation_manifest,
            },
        }
    finally:
        conn.close()


def make_handler(default_db_url: str | None, default_db_path: Path | None):
    """Create the HTTP handler class for the calculation API.

    Parameters
    ----------
    default_db_url : str or None
        Default PostgreSQL URL used when request does not provide ``db_url``.
    default_db_path : Path or None
        Default SQLite path used when request does not provide ``db``.

    Returns
    -------
    type[BaseHTTPRequestHandler]
        Request handler class implementing API routes.
    """
    class CalculationHandler(BaseHTTPRequestHandler):
        """HTTP handler for calculation-service routes."""

        server_version = "GrazeOpsCalculationService/0.1"

        def _send(self, code: int, payload: dict[str, Any]) -> None:
            """Send JSON response with status code."""
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_object(self) -> dict[str, Any]:
            """Read and validate JSON object body from request."""
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length > 0 else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception as exc:
                raise ValueError(f"invalid JSON body: {exc}") from exc
            if not isinstance(payload, dict):
                raise ValueError("JSON body must be an object")
            return payload

        def do_GET(self) -> None:  # noqa: N802
            """Handle calculation-service GET endpoints."""
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send(
                    200,
                    {
                        "status": "ok",
                        "service": "calculation-service",
                        "time": utc_now(),
                    },
                )
                return

            if parsed.path == "/recommendations/latest":
                query = parse_qs(parsed.query)
                boundary_id = str((query.get("boundary_id") or [""])[0]).strip()
                if not boundary_id:
                    self._send(400, {"error": "boundary_id query parameter is required"})
                    return

                calculation_date = str((query.get("calculation_date") or [""])[0]).strip() or None
                db_raw = str((query.get("db") or [""])[0]).strip()
                db_url_raw = str((query.get("db_url") or [""])[0]).strip()
                db_url = db_url_raw if db_url_raw else default_db_url
                db_path = Path(db_raw) if db_raw else default_db_path

                try:
                    recommendation = fetch_latest_recommendation(db_url, db_path, boundary_id, calculation_date)
                except Exception as exc:
                    self._send(400, {"error": str(exc)})
                    return

                if recommendation is None:
                    self._send(
                        404,
                        {
                            "error": "recommendation not found",
                            "boundary_id": boundary_id,
                            "calculation_date": calculation_date,
                        },
                    )
                    return

                self._send(200, {"status": "ok", "recommendation": recommendation})
                return

            if parsed.path == "/recommendations/explain":
                query = parse_qs(parsed.query)
                boundary_id = str((query.get("boundary_id") or [""])[0]).strip()
                calculation_date = str((query.get("calculation_date") or [""])[0]).strip() or None
                recommendation_id_raw = str((query.get("recommendation_id") or [""])[0]).strip()
                db_raw = str((query.get("db") or [""])[0]).strip()
                db_url_raw = str((query.get("db_url") or [""])[0]).strip()
                db_url = db_url_raw if db_url_raw else default_db_url
                db_path = Path(db_raw) if db_raw else default_db_path

                recommendation_id: int | None = None
                if recommendation_id_raw:
                    try:
                        recommendation_id = int(recommendation_id_raw)
                    except ValueError:
                        self._send(400, {"error": "recommendation_id must be an integer"})
                        return

                if recommendation_id is None and not boundary_id:
                    self._send(
                        400,
                        {
                            "error": "boundary_id is required unless recommendation_id is provided",
                        },
                    )
                    return

                try:
                    result = fetch_recommendation_explain(
                        db_url,
                        db_path,
                        boundary_id=boundary_id,
                        calculation_date=calculation_date,
                        recommendation_id=recommendation_id,
                    )
                except Exception as exc:
                    self._send(400, {"error": str(exc)})
                    return

                if result is None:
                    self._send(
                        404,
                        {
                            "error": "recommendation not found",
                            "boundary_id": boundary_id,
                            "calculation_date": calculation_date,
                            "recommendation_id": recommendation_id,
                        },
                    )
                    return

                self._send(200, {"status": "ok", **result})
                return

            self._send(404, {"error": f"unknown route: {parsed.path}"})

        def do_POST(self) -> None:  # noqa: N802
            """Handle calculation-service POST endpoints."""
            if self.path != "/calculate":
                self._send(404, {"error": f"unknown route: {self.path}"})
                return

            try:
                payload = self._read_json_object()
                args = build_run_args(payload)
                summary = run_calculation(args)
            except (ValueError, FileNotFoundError) as exc:
                self._send(400, {"error": str(exc)})
                return
            except Exception as exc:
                self._send(500, {"error": str(exc)})
                return

            self._send(200, {"status": "ok", "result": summary})

        def log_message(self, fmt: str, *args: Any) -> None:
            """Emit structured handler log line."""
            print(f"[{utc_now()}] calculation-service: " + (fmt % args))

    return CalculationHandler


def main() -> None:
    """Start the calculation HTTP service."""
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8081"))
    db_url = os.getenv("DATABASE_URL")
    db_path_raw = os.getenv("DB_PATH", "/data/grazeops.db")
    db_path = Path(db_path_raw) if db_path_raw else None
    handler = make_handler(db_url, db_path)
    server = HTTPServer((host, port), handler)
    target = db_url if db_url else str(db_path)
    print(f"[{utc_now()}] calculation-service: listening on {host}:{port} db={target}")
    server.serve_forever()


if __name__ == "__main__":
    main()
