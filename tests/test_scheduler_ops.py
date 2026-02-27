from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scheduler import build_ops_status


def _ts(minutes_ago: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _init_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE ingestion_run_metadata (
                ingestion_run_id TEXT PRIMARY KEY,
                scheduled_for TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                error TEXT,
                snapshot_id TEXT NOT NULL
            );

            CREATE TABLE calculation_runs (
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
            );
            """
        )


def test_build_ops_status_ok_when_recent_success_and_no_failures(tmp_path: Path) -> None:
    db_path = tmp_path / "ops_ok.db"
    _init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO ingestion_run_metadata (
                ingestion_run_id, scheduled_for, started_at, ended_at, status, error, snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("ing_ok_1", _ts(7), _ts(6), _ts(5), "success", None, "snap_1"),
        )
        conn.execute(
            """
            INSERT INTO calculation_runs (
                run_id, scheduled_for, boundary_id, calculation_date, model_version, config_version,
                status, started_at, ended_at, recommendation_id, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("calc_ok_1", _ts(9), "b1", "2024-03-15", "v1", "default", "success", _ts(8), _ts(7), 1, None),
        )
        conn.commit()

    status = build_ops_status(
        db_url=None,
        db_path=db_path,
        interval_seconds=300,
        max_failed_runs_24h=0,
        max_idle_seconds=900,
        stuck_run_minutes=30,
    )
    assert status["status"] == "ok"
    assert status["metrics"]["failed_runs_last_24h"]["total"] == 0
    assert status["metrics"]["stuck_runs"]["total"] == 0
    assert status["violations"] == []


def test_build_ops_status_degraded_for_failures_and_stuck_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "ops_bad.db"
    _init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO ingestion_run_metadata (
                ingestion_run_id, scheduled_for, started_at, ended_at, status, error, snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("ing_ok_1", _ts(12), _ts(11), _ts(10), "success", None, "snap_1"),
        )
        conn.execute(
            """
            INSERT INTO ingestion_run_metadata (
                ingestion_run_id, scheduled_for, started_at, ended_at, status, error, snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("ing_failed_1", _ts(4), _ts(4), _ts(3), "failed", "boom", "snap_2"),
        )
        conn.execute(
            """
            INSERT INTO ingestion_run_metadata (
                ingestion_run_id, scheduled_for, started_at, ended_at, status, error, snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("ing_stuck_1", _ts(80), _ts(80), None, "running", None, "snap_3"),
        )
        conn.execute(
            """
            INSERT INTO calculation_runs (
                run_id, scheduled_for, boundary_id, calculation_date, model_version, config_version,
                status, started_at, ended_at, recommendation_id, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("calc_failed_1", _ts(5), "b1", "2024-03-15", "v1", "default", "failed", _ts(5), _ts(4), None, "bad"),
        )
        conn.commit()

    status = build_ops_status(
        db_url=None,
        db_path=db_path,
        interval_seconds=300,
        max_failed_runs_24h=0,
        max_idle_seconds=900,
        stuck_run_minutes=30,
    )
    assert status["status"] == "degraded"
    assert status["metrics"]["failed_runs_last_24h"]["total"] == 2
    assert status["metrics"]["stuck_runs"]["total"] == 1
    assert any("failed_runs_last_24h" in item for item in status["violations"])
    assert any("stuck_runs" in item for item in status["violations"])
