#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shlex
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row


def utc_now() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Returns
    -------
    str
        Current time formatted as ``YYYY-mm-ddTHH:MM:SSZ``.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc_ts(value: str | None) -> datetime | None:
    """Parse a UTC timestamp string to a timezone-aware datetime.

    Parameters
    ----------
    value : str or None
        Timestamp text in ISO-8601 format. A trailing ``Z`` is supported.

    Returns
    -------
    datetime or None
        Parsed UTC datetime, or ``None`` when parsing fails.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except ValueError:
        return None


def seconds_since(value: str | None) -> int | None:
    """Compute elapsed seconds since a timestamp.

    Parameters
    ----------
    value : str or None
        UTC timestamp string.

    Returns
    -------
    int or None
        Non-negative elapsed seconds, or ``None`` when input is invalid.
    """
    ts = parse_utc_ts(value)
    if ts is None:
        return None
    delta = datetime.now(timezone.utc) - ts
    return max(int(delta.total_seconds()), 0)


def env_bool(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable.

    Parameters
    ----------
    name : str
        Environment variable name.
    default : bool, default=False
        Value used when the variable is missing.

    Returns
    -------
    bool
        Parsed boolean value.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def adapt_sql(sql: str, engine: str) -> str:
    """Adapt parameter placeholders for the active SQL engine.

    Parameters
    ----------
    sql : str
        SQL statement that may contain SQLite ``?`` placeholders.
    engine : str
        Database engine name.

    Returns
    -------
    str
        Engine-compatible SQL statement.
    """
    if engine != "postgres":
        return sql
    return sql.replace("?", "%s")


def table_exists(conn: Any, table_name: str, engine: str) -> bool:
    """Check whether a table exists.

    Parameters
    ----------
    conn : Any
        Database connection object.
    table_name : str
        Name of table to check.
    engine : str
        Database engine name (``postgres`` or ``sqlite``).

    Returns
    -------
    bool
        ``True`` if the table exists, otherwise ``False``.
    """
    if engine == "postgres":
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
    return row is not None


def build_ops_status(
    *,
    db_url: str | None,
    db_path: Path,
    interval_seconds: int,
    max_failed_runs_24h: int,
    max_idle_seconds: int,
    stuck_run_minutes: int,
) -> dict[str, Any]:
    """Build operational health status from recent run metadata.

    Parameters
    ----------
    db_url : str or None
        PostgreSQL connection URL. When set, this is preferred.
    db_path : Path
        SQLite database path used when ``db_url`` is not provided.
    interval_seconds : int
        Scheduler interval used to report current configuration.
    max_failed_runs_24h : int
        Threshold for failed ingestion/calculation runs in the last 24 hours.
    max_idle_seconds : int
        Maximum allowed age of last successful scheduler trigger.
    stuck_run_minutes : int
        Threshold for considering active runs as stuck.

    Returns
    -------
    dict[str, Any]
        Status payload for the ``/ops/status`` endpoint.
    """
    now_text = utc_now()
    now_dt = parse_utc_ts(now_text)
    if now_dt is None:
        now_dt = datetime.now(timezone.utc)
    since_24h = (now_dt - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    stuck_cutoff = (now_dt - timedelta(minutes=stuck_run_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")

    metrics: dict[str, Any] = {
        "failed_runs_last_24h": {
            "ingestion": None,
            "calculation": None,
            "total": None,
        },
        "last_successful_scheduler_trigger_at": None,
        "last_successful_scheduler_trigger_age_seconds": None,
        "stuck_runs": {
            "ingestion": None,
            "calculation": None,
            "total": None,
        },
    }

    violations: list[str] = []

    if not db_url and not db_path.exists():
        return {
            "status": "error",
            "time": now_text,
            "db_url": db_url,
            "db_path": str(db_path),
            "interval_seconds": interval_seconds,
            "thresholds": {
                "max_failed_runs_24h": max_failed_runs_24h,
                "max_idle_seconds": max_idle_seconds,
                "stuck_run_minutes": stuck_run_minutes,
            },
            "metrics": metrics,
            "violations": [f"db_missing:{db_path}"],
        }

    if db_url:
        conn = psycopg.connect(db_url, row_factory=dict_row)
        engine = "postgres"
    else:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        engine = "sqlite"

    try:

        failed_ingestion = 0
        if table_exists(conn, "ingestion_run_metadata", engine):
            row = conn.execute(
                adapt_sql(
                """
                SELECT COUNT(*) AS c
                FROM ingestion_run_metadata
                WHERE status = 'failed'
                  AND COALESCE(ended_at, started_at) >= ?
                """,
                engine,
                ),
                (since_24h,),
            ).fetchone()
            failed_ingestion = int(row["c"] if row is not None else 0)
            metrics["failed_runs_last_24h"]["ingestion"] = failed_ingestion

            row = conn.execute(
                adapt_sql(
                """
                SELECT MAX(COALESCE(ended_at, started_at)) AS ts
                FROM ingestion_run_metadata
                WHERE status = 'success'
                """,
                engine,
                ),
            ).fetchone()
            last_success = None if row is None else row["ts"]
            metrics["last_successful_scheduler_trigger_at"] = last_success
            metrics["last_successful_scheduler_trigger_age_seconds"] = seconds_since(
                None if last_success is None else str(last_success)
            )

            row = conn.execute(
                adapt_sql(
                """
                SELECT COUNT(*) AS c
                FROM ingestion_run_metadata
                WHERE status IN ('pending', 'running')
                  AND ended_at IS NULL
                  AND started_at <= ?
                """,
                engine,
                ),
                (stuck_cutoff,),
            ).fetchone()
            stuck_ingestion = int(row["c"] if row is not None else 0)
            metrics["stuck_runs"]["ingestion"] = stuck_ingestion

        failed_calculation = 0
        stuck_calculation = 0
        if table_exists(conn, "calculation_runs", engine):
            row = conn.execute(
                adapt_sql(
                """
                SELECT COUNT(*) AS c
                FROM calculation_runs
                WHERE status = 'failed'
                  AND COALESCE(ended_at, started_at) >= ?
                """,
                engine,
                ),
                (since_24h,),
            ).fetchone()
            failed_calculation = int(row["c"] if row is not None else 0)
            metrics["failed_runs_last_24h"]["calculation"] = failed_calculation

            row = conn.execute(
                adapt_sql(
                """
                SELECT COUNT(*) AS c
                FROM calculation_runs
                WHERE status IN ('pending', 'running')
                  AND ended_at IS NULL
                  AND started_at <= ?
                """,
                engine,
                ),
                (stuck_cutoff,),
            ).fetchone()
            stuck_calculation = int(row["c"] if row is not None else 0)
            metrics["stuck_runs"]["calculation"] = stuck_calculation
    finally:
        conn.close()

    failed_total = failed_ingestion + failed_calculation
    stuck_total = (metrics["stuck_runs"]["ingestion"] or 0) + (metrics["stuck_runs"]["calculation"] or 0)
    metrics["failed_runs_last_24h"]["total"] = failed_total
    metrics["stuck_runs"]["total"] = stuck_total

    if failed_total > max_failed_runs_24h:
        violations.append(f"failed_runs_last_24h:{failed_total}>{max_failed_runs_24h}")

    age_seconds = metrics["last_successful_scheduler_trigger_age_seconds"]
    if age_seconds is None:
        violations.append("last_successful_scheduler_trigger_age_seconds:missing")
    elif int(age_seconds) > max_idle_seconds:
        violations.append(f"last_successful_scheduler_trigger_age_seconds:{age_seconds}>{max_idle_seconds}")

    if stuck_total > 0:
        violations.append(f"stuck_runs:{stuck_total}>0")

    status = "ok" if not violations else "degraded"
    return {
        "status": status,
        "time": now_text,
        "db_url": db_url,
        "db_path": str(db_path),
        "interval_seconds": interval_seconds,
        "thresholds": {
            "max_failed_runs_24h": max_failed_runs_24h,
            "max_idle_seconds": max_idle_seconds,
            "stuck_run_minutes": stuck_run_minutes,
        },
        "metrics": metrics,
        "violations": violations,
    }


def build_command() -> list[str]:
    """Build the worker command for a scheduled execution.

    Returns
    -------
    list[str]
        Command tokens passed to ``subprocess.run``.
    """
    python_bin = os.getenv("PYTHON_BIN", "python")
    worker_entrypoint = os.getenv("WORKER_ENTRYPOINT", "/app/worker/main.py")
    worker_args = shlex.split(os.getenv("WORKER_ARGS", ""))
    return [python_bin, worker_entrypoint, *worker_args]


def run_once() -> int:
    """Execute one scheduled worker run.

    Returns
    -------
    int
        Process exit code from the worker command.
    """
    scheduled_for = utc_now()
    cmd = build_command()
    env = os.environ.copy()
    env["SCHEDULED_FOR"] = scheduled_for

    print(f"[{utc_now()}] scheduler: starting run for schedule={scheduled_for}")
    print(f"[{utc_now()}] scheduler: command={shlex.join(cmd)}")

    proc = subprocess.run(cmd, check=False, env=env)
    print(f"[{utc_now()}] scheduler: run finished with exit_code={proc.returncode}")
    return int(proc.returncode)


def make_handler(
    *,
    db_url: str | None,
    db_path: Path,
    interval_seconds: int,
    max_failed_runs_24h: int,
    max_idle_seconds: int,
    stuck_run_minutes: int,
):
    """Create the HTTP handler class for scheduler APIs.

    Parameters
    ----------
    db_url : str or None
        Optional PostgreSQL URL for ops checks.
    db_path : Path
        SQLite path fallback for ops checks.
    interval_seconds : int
        Scheduler interval setting.
    max_failed_runs_24h : int
        Failure threshold used by ``build_ops_status``.
    max_idle_seconds : int
        Idle threshold used by ``build_ops_status``.
    stuck_run_minutes : int
        Stuck-run threshold used by ``build_ops_status``.

    Returns
    -------
    type[BaseHTTPRequestHandler]
        Request handler class for ``/health`` and ``/ops/status``.
    """
    class SchedulerHandler(BaseHTTPRequestHandler):
        """HTTP handler for scheduler service routes."""

        server_version = "GrazeOpsScheduler/0.1"

        def _send(self, code: int, payload: dict[str, Any]) -> None:
            """Send JSON response with status code."""
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            """Handle scheduler GET endpoints."""
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send(
                    200,
                    {
                        "status": "ok",
                        "service": "scheduler",
                        "time": utc_now(),
                    },
                )
                return

            if parsed.path == "/ops/status":
                try:
                    payload = build_ops_status(
                        db_url=db_url,
                        db_path=db_path,
                        interval_seconds=interval_seconds,
                        max_failed_runs_24h=max_failed_runs_24h,
                        max_idle_seconds=max_idle_seconds,
                        stuck_run_minutes=stuck_run_minutes,
                    )
                    status_code = 200 if payload.get("status") == "ok" else 503
                    self._send(status_code, payload)
                except Exception as exc:
                    self._send(500, {"status": "error", "error": str(exc), "time": utc_now()})
                return

            self._send(404, {"error": f"unknown route: {parsed.path}"})

        def log_message(self, fmt: str, *args: Any) -> None:
            """Emit structured handler log line."""
            print(f"[{utc_now()}] scheduler-api: " + (fmt % args))

    return SchedulerHandler


def start_api_server(
    *,
    host: str,
    port: int,
    db_url: str | None,
    db_path: Path,
    interval_seconds: int,
    max_failed_runs_24h: int,
    max_idle_seconds: int,
    stuck_run_minutes: int,
) -> ThreadingHTTPServer:
    """Start the scheduler API server on a background thread.

    Parameters
    ----------
    host : str
        Bind host for HTTP server.
    port : int
        Bind port for HTTP server.
    db_url : str or None
        Optional PostgreSQL URL for ops checks.
    db_path : Path
        SQLite path fallback for ops checks.
    interval_seconds : int
        Scheduler interval setting.
    max_failed_runs_24h : int
        Failure threshold used by ``build_ops_status``.
    max_idle_seconds : int
        Idle threshold used by ``build_ops_status``.
    stuck_run_minutes : int
        Stuck-run threshold used by ``build_ops_status``.

    Returns
    -------
    ThreadingHTTPServer
        Running server instance.
    """
    handler = make_handler(
        db_url=db_url,
        db_path=db_path,
        interval_seconds=interval_seconds,
        max_failed_runs_24h=max_failed_runs_24h,
        max_idle_seconds=max_idle_seconds,
        stuck_run_minutes=stuck_run_minutes,
    )
    server = ThreadingHTTPServer((host, port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[{utc_now()}] scheduler: api listening on {host}:{port}")
    return server


def main() -> None:
    """Run scheduler loop and optional ops API server."""
    interval_seconds = int(
        os.getenv("INTERVAL_SECONDS", os.getenv("SCHEDULE_INTERVAL_SECONDS", "300"))
    )
    if interval_seconds < 1:
        raise ValueError("INTERVAL_SECONDS or SCHEDULE_INTERVAL_SECONDS must be >= 1")

    run_once_mode = env_bool("RUN_ONCE", False)
    api_enabled = env_bool("ENABLE_API", True)
    api_host = os.getenv("API_HOST", "0.0.0.0")
    api_port = int(os.getenv("API_PORT", "8082"))
    db_url = os.getenv("DATABASE_URL")
    db_path = Path(os.getenv("DB_PATH", "/data/grazeops.db"))
    max_failed_runs_24h = int(os.getenv("OPS_MAX_FAILED_RUNS_24H", "0"))
    max_idle_seconds = int(
        os.getenv(
            "OPS_MAX_TRIGGER_IDLE_SECONDS",
            str(max(interval_seconds * 3, 900)),
        )
    )
    stuck_run_minutes = int(os.getenv("OPS_STUCK_RUN_MINUTES", "30"))

    print(f"[{utc_now()}] scheduler: booted, interval_seconds={interval_seconds}, run_once={run_once_mode}")
    if api_enabled:
        start_api_server(
            host=api_host,
            port=api_port,
            db_url=db_url,
            db_path=db_path,
            interval_seconds=interval_seconds,
            max_failed_runs_24h=max_failed_runs_24h,
            max_idle_seconds=max_idle_seconds,
            stuck_run_minutes=stuck_run_minutes,
        )

    while True:
        run_once()
        if run_once_mode:
            return
        print(f"[{utc_now()}] scheduler: sleeping {interval_seconds}s")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
