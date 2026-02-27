#!/usr/bin/env python3
from __future__ import annotations

import json

from ingestion_worker.cli import parse_args
from ingestion_worker.worker import maybe_write_manifest, run_ingestion


def main() -> None:
    """Run ingestion worker entrypoint."""
    args = parse_args()
    summary = run_ingestion(args)
    maybe_write_manifest(summary, args.manifest_dir)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
