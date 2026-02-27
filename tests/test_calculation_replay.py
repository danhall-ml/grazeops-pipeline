from __future__ import annotations

import importlib.util
import sqlite3
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from calculation_service.worker import run_calculation


def _init_replay_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE geographic_boundaries (
                boundary_id TEXT PRIMARY KEY,
                area_ha REAL NOT NULL
            );

            CREATE TABLE herd_configurations (
                id TEXT PRIMARY KEY,
                boundary_id TEXT NOT NULL,
                animal_count INTEGER NOT NULL,
                daily_intake_kg_per_head REAL NOT NULL,
                config_snapshot_json TEXT,
                valid_from TEXT NOT NULL,
                valid_to TEXT
            );

            CREATE TABLE rap_biomass (
                boundary_id TEXT NOT NULL,
                composite_date TEXT NOT NULL,
                biomass_kg_per_ha REAL NOT NULL,
                source_version TEXT
            );

            CREATE TABLE weather_forecasts (
                boundary_id TEXT NOT NULL,
                forecast_date TEXT NOT NULL,
                precipitation_mm REAL,
                temp_max_c REAL
            );

            CREATE TABLE ingestion_runs (
                run_id TEXT PRIMARY KEY,
                boundary_id TEXT NOT NULL,
                timeframe_start TEXT NOT NULL,
                timeframe_end TEXT NOT NULL,
                sources_included TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT
            );

            CREATE TABLE ingestion_run_metadata (
                ingestion_run_id TEXT PRIMARY KEY,
                scheduled_for TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                error TEXT,
                snapshot_id TEXT NOT NULL
            );

            CREATE TABLE grazing_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                boundary_id TEXT NOT NULL,
                herd_config_id TEXT NOT NULL,
                calculation_date TEXT NOT NULL,
                available_forage_kg REAL NOT NULL,
                daily_consumption_kg REAL NOT NULL,
                days_of_grazing_remaining REAL NOT NULL,
                recommended_move_date TEXT NOT NULL,
                model_version TEXT NOT NULL,
                config_version TEXT,
                input_data_versions_json TEXT,
                created_at TEXT
            );
            """
        )

        conn.execute(
            "INSERT INTO geographic_boundaries (boundary_id, area_ha) VALUES (?, ?)",
            ("boundary_north_paddock_3", 45.2),
        )
        conn.execute(
            """
            INSERT INTO herd_configurations (
                id, boundary_id, animal_count, daily_intake_kg_per_head, config_snapshot_json, valid_from, valid_to
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "herd_cfg_1",
                "boundary_north_paddock_3",
                120,
                11.5,
                '{"herd":{"utilization_target_pct":50.0}}',
                "2024-01-01",
                None,
            ),
        )
        conn.execute(
            """
            INSERT INTO rap_biomass (boundary_id, composite_date, biomass_kg_per_ha, source_version)
            VALUES (?, ?, ?, ?)
            """,
            ("boundary_north_paddock_3", "2024-03-10", 800.0, "reference_db"),
        )
        for day, temp, precip in [
            ("2024-03-09", 29.0, 1.0),
            ("2024-03-10", 31.0, 0.5),
            ("2024-03-11", 30.5, 0.2),
            ("2024-03-12", 32.0, 0.1),
            ("2024-03-13", 33.0, 0.0),
            ("2024-03-14", 31.5, 0.1),
            ("2024-03-15", 31.0, 0.1),
        ]:
            conn.execute(
                """
                INSERT INTO weather_forecasts (boundary_id, forecast_date, precipitation_mm, temp_max_c)
                VALUES (?, ?, ?, ?)
                """,
                ("boundary_north_paddock_3", day, precip, temp),
            )

        conn.execute(
            """
            INSERT INTO ingestion_runs (
                run_id, boundary_id, timeframe_start, timeframe_end, sources_included, status, started_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ing_1",
                "boundary_north_paddock_3",
                "2024-01-01",
                "2024-12-31",
                "nrcs,rap,weather,herd",
                "completed",
                "2024-03-15T00:00:00Z",
                "2024-03-15T00:05:00Z",
            ),
        )
        conn.execute(
            """
            INSERT INTO ingestion_run_metadata (
                ingestion_run_id, scheduled_for, started_at, ended_at, status, error, snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ing_1",
                "2024-03-15T00:00:00Z",
                "2024-03-15T00:00:05Z",
                "2024-03-15T00:05:00Z",
                "success",
                None,
                "snap_20240315",
            ),
        )
        conn.commit()


def _load_calculation_main_module():
    root = Path(__file__).resolve().parents[1]
    path = root / "services" / "calculation-service" / "main.py"
    spec = importlib.util.spec_from_file_location("calculation_main_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module spec from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_calculation_replay_is_deterministic_and_explainable(tmp_path: Path) -> None:
    db_path = tmp_path / "replay.db"
    _init_replay_db(db_path)

    base_args = {
        "db": db_path,
        "db_url": None,
        "boundary_id": "boundary_north_paddock_3",
        "calculation_date": "2024-03-15",
        "calculation_date_obj": date(2024, 3, 15),
        "model_version": "v2",
        "config_version": "default",
        "utilization_target_pct": None,
        "scheduled_for": None,
        "register_model": False,
        "registry_dir": None,
        "registry_url": None,
    }

    first = run_calculation(SimpleNamespace(run_id="calc_replay_1", **base_args))
    second = run_calculation(SimpleNamespace(run_id="calc_replay_2", **base_args))

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
        assert first[field] == second[field]
    assert first["decision_snapshot_id"] == second["decision_snapshot_id"]
    assert first["recommendation_id"] != second["recommendation_id"]

    with sqlite3.connect(db_path) as conn:
        n_rows = conn.execute("SELECT COUNT(*) FROM grazing_recommendations").fetchone()[0]
        manifest_count = conn.execute("SELECT COUNT(*) FROM calculation_manifests").fetchone()[0]
        manifest_row = conn.execute(
            "SELECT manifest_json FROM calculation_manifests WHERE decision_snapshot_id=?",
            (first["decision_snapshot_id"],),
        ).fetchone()
    assert n_rows == 2
    assert manifest_count == 1
    assert manifest_row is not None

    calculation_main = _load_calculation_main_module()
    explain = calculation_main.fetch_recommendation_explain(
        None,
        db_path,
        boundary_id="",
        calculation_date=None,
        recommendation_id=int(second["recommendation_id"]),
    )
    assert explain is not None
    assert explain["recommendation"]["recommendation_id"] == second["recommendation_id"]
    assert explain["lineage"]["input_data_versions"]["snapshot_id"] == second["snapshot_id"]
    manifest_lineage = explain["lineage"]["calculation_manifest"]
    assert manifest_lineage is not None
    assert manifest_lineage["decision_snapshot_id"] == second["decision_snapshot_id"]
