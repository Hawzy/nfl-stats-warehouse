"""Allow running `python -m nfl_warehouse`."""

import sys

from nfl_warehouse.cli import main

if __name__ == "__main__":
    sys.exit(main())
