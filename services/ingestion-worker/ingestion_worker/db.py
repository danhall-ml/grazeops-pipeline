from __future__ import annotations

import json
from typing import Any

from .util import parse_utc_ts, utc_after, utc_now


def add_quality_check(
    conn: Any,
    run_id: str,
    check_name: str,
    check_type: str,
    passed: bool,
    details: dict[str, Any],
) -> None:
    """Insert one ingestion quality-check result.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    run_id : str
        Ingestion run identifier.
    check_name : str
        Quality check name.
    check_type : str
        Check category (for example ``ingestion`` or ``freshness``).
    passed : bool
        Check pass/fail flag.
    details : dict[str, Any]
        Structured check details serialized to JSON.
    """
    conn.execute(
        """
        INSERT INTO data_quality_checks (run_id, check_name, check_type, passed, details_json, checked_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            check_name,
            check_type,
            1 if passed else 0,
            json.dumps(details, sort_keys=True),
            utc_now(),
        ),
    )


def ensure_run_metadata_table(conn: Any) -> None:
    """Ensure ingestion run metadata table exists.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
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


def ensure_run_lock_table(conn: Any) -> None:
    """Ensure ingestion run lock table exists.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_run_lock (
            boundary_id TEXT PRIMARY KEY,
            lock_owner TEXT NOT NULL,
            lock_until TEXT NOT NULL,
            acquired_at TEXT NOT NULL
        )
        """
    )


def acquire_lock(
    conn: Any,
    boundary_id: str,
    run_id: str,
    lock_ttl_minutes: int,
) -> bool:
    """Acquire or refresh boundary-scoped ingestion lock.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    boundary_id : str
        Boundary identifier to lock.
    run_id : str
        Current run identifier requesting lock ownership.
    lock_ttl_minutes : int
        Lock lease duration in minutes.

    Returns
    -------
    bool
        ``True`` when lock is acquired, ``False`` when another active owner exists.
    """
    now = utc_now()
    now_dt = parse_utc_ts(now)
    lock_until = utc_after(lock_ttl_minutes)

    if getattr(conn, "engine", "sqlite") == "sqlite":
        conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        "SELECT lock_owner, lock_until FROM ingestion_run_lock WHERE boundary_id = ?",
        (boundary_id,),
    ).fetchone()

    if row is not None:
        owner = str(row["lock_owner"])
        until_text = str(row["lock_until"])
        still_valid = parse_utc_ts(until_text) > now_dt
        if still_valid and owner != run_id:
            conn.rollback()
            return False

    conn.execute(
        """
        INSERT INTO ingestion_run_lock (boundary_id, lock_owner, lock_until, acquired_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(boundary_id) DO UPDATE SET
            lock_owner=excluded.lock_owner,
            lock_until=excluded.lock_until,
            acquired_at=excluded.acquired_at
        """,
        (boundary_id, run_id, lock_until, now),
    )
    conn.commit()
    return True


def release_lock(conn: Any, boundary_id: str, run_id: str) -> None:
    """Release boundary lock for a specific run owner.

    Parameters
    ----------
    conn : Any
        Operational database connection wrapper.
    boundary_id : str
        Boundary identifier.
    run_id : str
        Lock owner run identifier.
    """
    conn.execute(
        "DELETE FROM ingestion_run_lock WHERE boundary_id = ? AND lock_owner = ?",
        (boundary_id, run_id),
    )
    conn.commit()
