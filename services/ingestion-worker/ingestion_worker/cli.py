from __future__ import annotations

import argparse
import os
from pathlib import Path

from .util import parse_date


def env_bool(name: str, default: bool) -> bool:
    """Read a boolean environment variable.

    Parameters
    ----------
    name : str
        Environment variable name.
    default : bool
        Value used when variable is missing.

    Returns
    -------
    bool
        Parsed boolean value.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    """Read an integer environment variable.

    Parameters
    ----------
    name : str
        Environment variable name.
    default : int
        Value used when variable is missing or invalid.

    Returns
    -------
    int
        Parsed integer value.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_args() -> argparse.Namespace:
    """Parse and validate ingestion worker CLI arguments.

    Returns
    -------
    argparse.Namespace
        Parsed CLI arguments including validated date objects.

    Raises
    ------
    ValueError
        Raised when ``end-date`` is earlier than ``start-date``.
    """
    parser = argparse.ArgumentParser(description="Task 2 ingestion worker")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(os.getenv("DB_PATH", "/data/grazeops.db")),
        help="Target DB path (used when DATABASE_URL is not set)",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL"),
        help="Target PostgreSQL connection URL",
    )
    parser.add_argument(
        "--source-db",
        type=Path,
        default=Path(os.getenv("SOURCE_DB_PATH", "/inputs/pasture_reference.db")),
        help="Reference source SQLite DB path",
    )
    parser.add_argument(
        "--boundary-path",
        type=Path,
        default=Path(os.getenv("BOUNDARY_PATH", "/inputs/sample_boundary.geojson")),
        help="Boundary GeoJSON path",
    )
    parser.add_argument(
        "--herd-path",
        type=Path,
        default=Path(os.getenv("HERD_PATH", "/inputs/sample_herds_pasturemap.json")),
        help="PastureMap herd JSON path",
    )
    parser.add_argument(
        "--start-date",
        default=os.getenv("START_DATE", "2024-01-01"),
        help="Window start date YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        default=os.getenv("END_DATE", "2024-12-31"),
        help="Window end date YYYY-MM-DD",
    )
    parser.add_argument("--boundary-id", default=os.getenv("BOUNDARY_ID"), help="Optional explicit boundary ID")
    parser.add_argument("--run-id", default=os.getenv("RUN_ID"), help="Optional explicit run ID")
    parser.add_argument(
        "--scheduled-for",
        default=os.getenv("SCHEDULED_FOR"),
        help="Optional scheduler timestamp",
    )
    manifest_dir = os.getenv("MANIFEST_DIR")
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default=Path(manifest_dir) if manifest_dir else None,
        help="Optional JSON manifest output dir",
    )
    parser.add_argument(
        "--wait-for-db-seconds",
        type=int,
        default=env_int("WAIT_FOR_DB_SECONDS", 30),
        help="Wait timeout for target DB file existence",
    )
    parser.add_argument(
        "--lock-ttl-minutes",
        type=int,
        default=env_int("LOCK_TTL_MINUTES", 30),
        help="Lease duration for run lock",
    )
    parser.add_argument(
        "--rap-stale-days",
        type=int,
        default=env_int("RAP_STALE_DAYS", 32),
        help="Alert threshold for RAP staleness in days",
    )
    parser.add_argument(
        "--prefer-openmeteo",
        action="store_true",
        default=env_bool("PREFER_OPENMETEO", False),
        help="Try OpenMeteo first for weather; fallback to reference DB if unavailable",
    )
    parser.add_argument(
        "--backfill-weather",
        action="store_true",
        default=env_bool("BACKFILL_WEATHER", False),
        help="Backfill missing weather days in window",
    )
    args = parser.parse_args()

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if end_date < start_date:
        raise ValueError("end-date must be >= start-date")
    args.start_date_obj = start_date
    args.end_date_obj = end_date
    return args
