from __future__ import annotations

from datetime import date, datetime, timezone


def utc_now() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Returns
    -------
    str
        Current time formatted as ``YYYY-mm-ddTHH:MM:SSZ``.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
