from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


class OperationalDB:
    """Small DB wrapper providing engine-aware SQL execution."""

    def __init__(self, engine: str, conn: Any) -> None:
        """Initialize operational DB wrapper.

        Parameters
        ----------
        engine : str
            Database engine name (``sqlite`` or ``postgres``).
        conn : Any
            Native DB connection object.
        """
        self.engine = engine
        self._conn = conn

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        """Execute SQL with engine-specific placeholder adaptation.

        Parameters
        ----------
        sql : str
            SQL statement.
        params : tuple[Any, ...], default=()
            Query parameters.

        Returns
        -------
        Any
            Driver-specific cursor/result object.
        """
        statement = _adapt_sql(sql, self.engine)
        return self._conn.execute(statement, params)

    def commit(self) -> None:
        """Commit current transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Rollback current transaction."""
        self._conn.rollback()

    def close(self) -> None:
        """Close underlying connection."""
        self._conn.close()


def _adapt_sql(sql: str, engine: str) -> str:
    """Adapt SQL placeholders for the selected engine.

    Parameters
    ----------
    sql : str
        SQL statement potentially using SQLite placeholders.
    engine : str
        Engine name.

    Returns
    -------
    str
        Engine-compatible SQL statement.
    """
    if engine != "postgres":
        return sql
    statement = sql
    statement = statement.replace("BEGIN IMMEDIATE", "BEGIN")
    statement = statement.replace("?", "%s")
    return statement


def connect_operational_db(*, db_url: str | None, db_path: Path | None) -> OperationalDB:
    """Connect to PostgreSQL or SQLite operational database.

    Parameters
    ----------
    db_url : str or None
        PostgreSQL connection URL.
    db_path : Path or None
        SQLite path fallback when ``db_url`` is not set.

    Returns
    -------
    OperationalDB
        Wrapped operational DB connection.

    Raises
    ------
    ValueError
        Raised when neither ``db_url`` nor ``db_path`` is usable.
    """
    if db_url:
        conn = psycopg.connect(db_url, row_factory=dict_row)
        return OperationalDB("postgres", conn)

    if db_path is None:
        raise ValueError("db_path is required when DATABASE_URL is not set")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return OperationalDB("sqlite", conn)
