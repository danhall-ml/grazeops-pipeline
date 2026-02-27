from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# Make service packages importable in tests.
sys.path.insert(0, str(ROOT / "services" / "calculation-service"))
sys.path.insert(0, str(ROOT / "services" / "ingestion-worker"))
sys.path.insert(0, str(ROOT / "services" / "scheduler"))
