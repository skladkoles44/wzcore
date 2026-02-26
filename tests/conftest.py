from __future__ import annotations

import sys
from pathlib import Path

# Ensure "src/" is importable for tests (CI uses PYTHONPATH=src; local runs may not).
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
