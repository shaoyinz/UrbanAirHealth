"""Add src/ to sys.path so `airhealth` is importable without an install.

Phase 1 keeps the project pyproject.toml-free per the operator's
preference (see memory/feedback_minimal_setup). This conftest is the
seam that lets pytest discover the package from a bare uv venv.
"""

from __future__ import annotations

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
