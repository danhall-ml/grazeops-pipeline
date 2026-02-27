from __future__ import annotations

import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Returns
    -------
    str
        Current time formatted as ``YYYY-mm-ddTHH:MM:SSZ``.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc_ts(text: str) -> datetime:
    """Parse a UTC timestamp string.

    Parameters
    ----------
    text : str
        Timestamp formatted as ``YYYY-mm-ddTHH:MM:SSZ``.

    Returns
    -------
    datetime
        Parsed timezone-aware UTC datetime.
    """
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def utc_after(minutes: int) -> str:
    """Return a UTC timestamp offset into the future.

    Parameters
    ----------
    minutes : int
        Number of minutes to add to current UTC time.

    Returns
    -------
    str
        Future timestamp formatted as ``YYYY-mm-ddTHH:MM:SSZ``.
    """
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_date(text: str) -> date:
    """Parse an ISO date string.

    Parameters
    ----------
    text : str
        Date string in ``YYYY-mm-dd`` format.

    Returns
    -------
    date
        Parsed date value.
    """
    return date.fromisoformat(text)


def date_iter(start: date, end: date) -> list[date]:
    """Generate inclusive daily dates between two bounds.

    Parameters
    ----------
    start : date
        Inclusive start date.
    end : date
        Inclusive end date.

    Returns
    -------
    list[date]
        Ordered list of dates from ``start`` to ``end``.
    """
    out: list[date] = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def slugify(text: str) -> str:
    """Convert text into a lowercase underscore identifier.

    Parameters
    ----------
    text : str
        Input text.

    Returns
    -------
    str
        Slugified identifier with non-alphanumeric characters replaced by ``_``.
    """
    chars = []
    for ch in text.lower():
        if ch.isalnum():
            chars.append(ch)
        else:
            chars.append("_")
    return "".join(chars).strip("_")


def stable_hash(payload: Any) -> str:
    """Compute a stable SHA-256 hash for a JSON-serializable payload.

    Parameters
    ----------
    payload : Any
        JSON-serializable object.

    Returns
    -------
    str
        Hex digest of the canonical JSON representation.
    """
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def wait_for_file(path: Path, timeout_seconds: int) -> None:
    """Wait until a file exists or timeout.

    Parameters
    ----------
    path : Path
        File path to wait for.
    timeout_seconds : int
        Maximum wait duration in seconds.

    Raises
    ------
    FileNotFoundError
        Raised when the file does not appear before timeout.
    """
    if path.exists():
        return
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if path.exists():
            return
        time.sleep(1)
    raise FileNotFoundError(f"Timed out waiting for file: {path}")
