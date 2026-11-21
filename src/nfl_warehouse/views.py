"""Portable DuckDB nfl.* views over relative Parquet paths."""

from pathlib import Path
from typing import Callable

import duckdb

from nfl_warehouse.catalog import Dataset
from nfl_warehouse.download import parquet_path
from nfl_warehouse.paths import WarehousePaths

__all__ = ["create_views", "set_file_search_path"]


def set_file_search_path(con: duckdb.DuckDBPyConnection, data_dir: Path) -> None:
    """Configure DuckDB file_search_path so relative nfl.* views resolve portably."""
    posix_dir = Path(data_dir).resolve().as_posix().replace("'", "''")
    con.execute(f"SET GLOBAL file_search_path = '{posix_dir}'")


def _relative_nflverse_prefix(paths: WarehousePaths) -> str:
    """Compute POSIX path of nflverse_dir relative to data_dir."""
    return paths.nflverse_dir.resolve().relative_to(paths.data_dir.resolve()).as_posix()


def create_views(
    con: duckdb.DuckDBPyConnection,
    paths: WarehousePaths,
    datasets: list[Dataset],
    log: Callable[[str], None] = print,
) -> list[str]:
    """Create portable nfl.* views using relative paths + DuckDB file_search_path."""
    set_file_search_path(con, paths.data_dir)
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl")
    rel_root = _relative_nflverse_prefix(paths)
    created: list[str] = []

    for ds in datasets:
        if ds.seasonal:
            ds_dir = paths.nflverse_dir / ds.name
            parquet_files = (
                [
                    p
                    for p in ds_dir.glob("*.parquet")
                    if p.is_file() and p.suffix == ".parquet"
                ]
                if ds_dir.is_dir()
                else []
            )
            if not parquet_files:
                log(f"Skipping nfl.{ds.name}: no parquet files found on disk")
                continue
            rel_target = f"{rel_root}/{ds.name}/*.parquet"
        else:
            target_file = parquet_path(paths, ds, None)
            if not (target_file.is_file() and target_file.suffix == ".parquet"):
                log(f"Skipping nfl.{ds.name}: no parquet files found on disk")
                continue
            rel_target = f"{rel_root}/{ds.name}/latest.parquet"

        escaped = rel_target.replace("'", "''")
        sql = (
            f"CREATE OR REPLACE VIEW nfl.{ds.name} AS "
            f"SELECT * FROM read_parquet('{escaped}', union_by_name = true)"
        )
        con.execute(sql)
        created.append(ds.name)

    return created
