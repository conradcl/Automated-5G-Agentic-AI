from __future__ import annotations

import sys
from pathlib import Path


RAPP_ROOT = Path(__file__).resolve().parents[1]
if str(RAPP_ROOT) not in sys.path:
    sys.path.insert(0, str(RAPP_ROOT))
