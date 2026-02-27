#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for SQLite schema initialization.

    Returns
    -------
    argparse.Namespace
        Parsed CLI argument namespace.
    """
    parser = argparse.ArgumentParser(
        description="Initialize GrazeOps SQLite database from schema.sql"
    )
    parser.add_argument(
        "--schema",
        type=Path,
        required=True,
        help="Path to schema.sql",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("/data/grazeops.db"),
        help="SQLite database file path",
    )
    return parser.parse_args()


def init_db(schema_path: Path, db_path: Path) -> None:
    """Initialize SQLite database from schema file.

    Parameters
    ----------
    schema_path : Path
        Path to SQL schema file.
    db_path : Path
        Target SQLite database path.

    Raises
    ------
    FileNotFoundError
        Raised when schema file does not exist.
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    schema_sql = schema_path.read_text(encoding="utf-8")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema_sql)
        conn.commit()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()

    table_names = [row[0] for row in tables]
    print(f"Database initialized: {db_path}")
    print(f"Tables created/verified ({len(table_names)}):")
    for name in table_names:
        print(f"- {name}")


def main() -> None:
    """Run SQLite initialization entrypoint."""
    args = parse_args()
    init_db(schema_path=args.schema, db_path=args.db)


if __name__ == "__main__":
    main()
