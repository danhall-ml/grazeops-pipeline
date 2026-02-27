# sqlite-db

## Purpose

This helper service initializes a SQLite database from a schema file. It is kept for local/legacy workflows; main stack runtime now uses PostgreSQL.

## API / Inputs / Outputs

- Interface: CLI (`python /app/init_sqlite.py --schema ... --db ...`)
- Inputs:
  - SQL schema file path
  - Output SQLite DB path
- Output:
  - SQLite database file with initialized tables

## Required Environment Variables

None required. Use CLI flags.

## Local Run

From this service directory:

```bash
python3 init_sqlite.py --schema ../../inputs/schema.sql --db /tmp/grazeops.db
```

## Smoke Check / Health

Script exits with code `0` and prints created/verified table names.

## Dependencies

- Requires a valid schema SQL file.
