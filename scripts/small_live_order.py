"""Submit one guarded small-live order.

This script can place a real Spot order only when the readiness gate passes,
credentials are present in the local environment, and the explicit live-order
confirmation flag is provided.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.live.order_cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
