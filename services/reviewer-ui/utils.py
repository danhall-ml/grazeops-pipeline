from __future__ import annotations

import datetime as dt
import os
import shlex
import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

WORKSPACE_ROOT = Path(os.getenv("WORKSPACE_ROOT", "/workspace"))
DB_URL = os.getenv("DATABASE_URL")
DB_PATH = Path(os.getenv("DB_PATH", "/data/grazeops.db"))
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://model-registry:8080")
CALCULATION_URL = os.getenv("CALCULATION_URL", "http://calculation-service:8081")
SCHEDULER_URL = os.getenv("SCHEDULER_URL", "http://scheduler:8082")

DEFAULT_SOURCE_DB = os.getenv("SOURCE_DB_PATH", "/inputs/pasture_reference.db")
DEFAULT_BOUNDARY_PATH = os.getenv("BOUNDARY_PATH", "/inputs/sample_boundary.geojson")
DEFAULT_HERD_PATH = os.getenv("HERD_PATH", "/inputs/sample_herds_pasturemap.json")
DEFAULT_START_DATE = os.getenv("START_DATE", "2024-01-01")
DEFAULT_END_DATE = os.getenv("END_DATE", "2024-12-31")
DEFAULT_MODEL_VERSION = os.getenv("MODEL_VERSION", "v1")
DEFAULT_CONFIG_VERSION = os.getenv("CONFIG_VERSION", "default")


def parse_date_or_default(text: str, fallback: dt.date) -> dt.date:
    """Parse date text with fallback.

    Parameters
    ----------
    text : str
        Date string in ``YYYY-mm-dd`` format.
    fallback : dt.date
        Default date used when parsing fails.

    Returns
    -------
    dt.date
        Parsed date or fallback value.
    """
    try:
        return dt.date.fromisoformat(text)
    except Exception:
        return fallback


def db_exists() -> bool:
    """Check whether operational DB is reachable.

    Returns
    -------
    bool
        ``True`` when configured DB can be reached.
    """
    if DB_URL:
        try:
            with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
                conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False
    return DB_PATH.exists()


def _adapt_sql(sql: str, use_postgres: bool) -> str:
    """Adapt SQL placeholders for PostgreSQL when needed.

    Parameters
    ----------
    sql : str
        SQL statement using SQLite-style placeholders.
    use_postgres : bool
        Flag indicating PostgreSQL mode.

    Returns
    -------
    str
        Engine-compatible SQL statement.
    """
    if not use_postgres:
        return sql
    return sql.replace("?", "%s")


def _query_one(sql: str, params: tuple[Any, ...] = ()) -> Any:
    """Execute a single-row query against configured operational DB.

    Parameters
    ----------
    sql : str
        SQL statement.
    params : tuple[Any, ...], default=()
        Query parameters.

    Returns
    -------
    Any
        Row object or ``None`` when no row exists.
    """
    if DB_URL:
        with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
            return conn.execute(_adapt_sql(sql, True), params).fetchone()

    import sqlite3

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(sql, params).fetchone()


def query_rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Execute a multi-row query against the configured operational DB.

    Parameters
    ----------
    sql : str
        SQL statement using SQLite-style placeholders.
    params : tuple[Any, ...], default=()
        Query parameters.

    Returns
    -------
    list[dict[str, Any]]
        Result rows converted to dictionaries.
    """
    if DB_URL:
        with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
            rows = conn.execute(_adapt_sql(sql, True), params).fetchall()
            return [dict(row) for row in rows]

    import sqlite3

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]


def get_default_boundary_id() -> str:
    """Return default boundary id for reviewer presets.

    Returns
    -------
    str
        Boundary identifier.
    """
    if not db_exists():
        return "boundary_north_paddock_3"
    try:
        row = _query_one(
            "SELECT boundary_id FROM geographic_boundaries ORDER BY boundary_id LIMIT 1"
        )
    except Exception:
        return "boundary_north_paddock_3"
    if row is None:
        return "boundary_north_paddock_3"
    return str(row["boundary_id"] or "boundary_north_paddock_3")


def get_default_calc_date(boundary_id: str) -> dt.date:
    """Return default calculation date for reviewer presets.

    Parameters
    ----------
    boundary_id : str
        Boundary identifier used to infer latest RAP date.

    Returns
    -------
    dt.date
        Suggested calculation date.
    """
    fallback = dt.date(2024, 3, 15)
    if not db_exists():
        return fallback
    try:
        row = _query_one(
            "SELECT max(composite_date) AS composite_date FROM rap_biomass WHERE boundary_id = ?",
            (boundary_id,),
        )
    except Exception:
        return fallback

    if row is None or row["composite_date"] is None:
        return fallback
    return parse_date_or_default(str(row["composite_date"]), fallback)


def run_command(
    command: list[str],
    timeout_seconds: int = 300,
    extra_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run a local command and capture stdout/stderr.

    Parameters
    ----------
    command : list[str]
        Command tokens.
    timeout_seconds : int, default=300
        Process timeout in seconds.
    extra_env : dict[str, str] or None, default=None
        Optional environment variable overrides.

    Returns
    -------
    dict[str, Any]
        Execution result payload including return code and streams.
    """
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    try:
        completed = subprocess.run(
            command,
            cwd=str(WORKSPACE_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
            env=env,
        )
        return {
            "command": shlex.join(command),
            "returncode": completed.returncode,
            "stdout": completed.stdout or "",
            "stderr": completed.stderr or "",
            "timed_out": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": shlex.join(command),
            "returncode": 124,
            "stdout": exc.stdout or "",
            "stderr": (exc.stderr or "") + f"\nTimed out after {timeout_seconds} seconds.",
            "timed_out": True,
        }


def run_http_json(
    url: str,
    payload: dict[str, Any],
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Run HTTP POST JSON request and capture result payload.

    Parameters
    ----------
    url : str
        Request URL.
    payload : dict[str, Any]
        JSON request body.
    timeout_seconds : int, default=120
        Request timeout in seconds.

    Returns
    -------
    dict[str, Any]
        Result payload including return code and response/error text.
    """
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
        return {
            "command": f"POST {url}",
            "returncode": 0,
            "stdout": body,
            "stderr": "",
            "timed_out": False,
        }
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        return {
            "command": f"POST {url}",
            "returncode": int(exc.code or 1),
            "stdout": "",
            "stderr": error_body or str(exc),
            "timed_out": False,
        }
    except Exception as exc:
        return {
            "command": f"POST {url}",
            "returncode": 1,
            "stdout": "",
            "stderr": str(exc),
            "timed_out": False,
        }


def run_http_get(
    url: str,
    query_params: dict[str, Any] | None = None,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Run HTTP GET request with optional query params.

    Parameters
    ----------
    url : str
        Base request URL.
    query_params : dict[str, Any] or None, default=None
        Query parameter dictionary.
    timeout_seconds : int, default=120
        Request timeout in seconds.

    Returns
    -------
    dict[str, Any]
        Result payload including return code and response/error text.
    """
    encoded = {}
    for key, value in (query_params or {}).items():
        if value in (None, ""):
            continue
        encoded[str(key)] = str(value)

    query = urllib.parse.urlencode(encoded)
    full_url = f"{url}?{query}" if query else url
    req = urllib.request.Request(url=full_url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
        return {
            "command": f"GET {full_url}",
            "returncode": 0,
            "stdout": body,
            "stderr": "",
            "timed_out": False,
        }
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        return {
            "command": f"GET {full_url}",
            "returncode": int(exc.code or 1),
            "stdout": "",
            "stderr": error_body or str(exc),
            "timed_out": False,
        }
    except Exception as exc:
        return {
            "command": f"GET {full_url}",
            "returncode": 1,
            "stdout": "",
            "stderr": str(exc),
            "timed_out": False,
        }
