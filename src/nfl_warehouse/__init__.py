"""Public DuckDB + nflverse Parquet NFL analytics warehouse."""

from nfl_warehouse.advanced_stats import (
    build_advanced_stats_tables,
    build_player_week_adv,
    build_player_week_xfp,
    build_team_week_dvp,
    impute_constrained_routes,
)
from nfl_warehouse.catalog import CATALOG, VALID_PROFILES, Dataset, get_datasets
from nfl_warehouse.connect import connect, query_polars
from nfl_warehouse.paths import WarehousePaths

__all__ = [
    "CATALOG",
    "Dataset",
    "VALID_PROFILES",
    "WarehousePaths",
    "build_advanced_stats_tables",
    "build_player_week_adv",
    "build_player_week_xfp",
    "build_team_week_dvp",
    "connect",
    "get_datasets",
    "impute_constrained_routes",
    "query_polars",
]

__version__ = "0.1.0"
