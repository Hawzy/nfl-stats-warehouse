"""Thin CLI wrapper to hydrate or rebuild the NFL stats warehouse."""

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.cli import main

if __name__ == "__main__":
    sys.exit(main())
