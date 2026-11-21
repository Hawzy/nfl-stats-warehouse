"""Public DuckDB + nflverse Parquet NFL analytics warehouse."""

from nfl_warehouse.catalog import CATALOG, VALID_PROFILES, Dataset, get_datasets
from nfl_warehouse.connect import connect, query_polars
from nfl_warehouse.paths import WarehousePaths

__all__ = [
    "CATALOG",
    "Dataset",
    "VALID_PROFILES",
    "WarehousePaths",
    "connect",
    "get_datasets",
    "query_polars",
]

__version__ = "0.1.0"
