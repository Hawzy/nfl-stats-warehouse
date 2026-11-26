"""DuckDB connection provider with portable search path and read-only attachments."""

from pathlib import Path
import re
import threading
from typing import Any, Callable, Mapping, Sequence

import duckdb
import polars as pl

from nfl_warehouse.paths import WarehousePaths
from nfl_warehouse.views import set_file_search_path

__all__ = ["connect", "query_polars"]

_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DUCKDB_SUFFIXES = {".duckdb", ".ddb"}
_ATTACH_LOCK = threading.Lock()


def _load_sqlite_extension(con: duckdb.DuckDBPyConnection) -> None:
    """Load DuckDB's sqlite extension, installing it once if not yet cached."""
    try:
        con.execute("LOAD sqlite;")
    except duckdb.Error:
        con.execute("INSTALL sqlite; LOAD sqlite;")


def connect(
    paths: WarehousePaths | None = None,
    *,
    read_only: bool = False,
    attach: Mapping[str, Path | str] | None = None,
    log: Callable[[str], None] = print,
) -> duckdb.DuckDBPyConnection:
    """Open DuckDB warehouse connection with portable views and optional READ_ONLY DBs."""
    if paths is None:
        paths = WarehousePaths.default()

    if read_only:
        if not paths.warehouse_path.exists():
            raise FileNotFoundError(
                f"Warehouse database not found at {paths.warehouse_path}. "
                "Run 'python -m nfl_warehouse --profile lite' (or 'nfl-warehouse') to initialize."
            )
    else:
        paths.warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(paths.warehouse_path), read_only=read_only)
    try:
        set_file_search_path(con, paths.data_dir)

        if attach:
            with _ATTACH_LOCK:
                sqlite_loaded = False
                for alias, raw_target in attach.items():
                    if not _ALIAS_RE.fullmatch(alias):
                        raise ValueError(
                            f"Invalid attachment alias {alias!r}; must match [A-Za-z_][A-Za-z0-9_]*"
                        )
                    target = Path(raw_target).expanduser()
                    if not target.exists():
                        log(f"Skipping attachment '{alias}': {target} not found")
                        continue

                    escaped = target.resolve().as_posix().replace("'", "''")
                    if target.suffix.lower() in _DUCKDB_SUFFIXES:
                        con.execute(
                            f"ATTACH IF NOT EXISTS '{escaped}' AS {alias} (READ_ONLY)"
                        )
                    else:
                        if not sqlite_loaded:
                            _load_sqlite_extension(con)
                            sqlite_loaded = True
                        con.execute(
                            f"ATTACH IF NOT EXISTS '{escaped}' AS {alias} (TYPE sqlite, READ_ONLY)"
                        )
    except Exception:
        con.close()
        raise

    return con


def query_polars(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: Sequence[Any] | None = None,
) -> pl.DataFrame:
    """Execute SQL on an isolated child cursor and return a Polars DataFrame."""
    cur = con.cursor()
    try:
        try:
            rel = cur.sql(sql, params=params) if params is not None else cur.sql(sql)
            return rel.pl()
        except ModuleNotFoundError:
            if params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cols = [d[0] for d in (cur.description or [])]
            rows = cur.fetchall()
            return pl.DataFrame(rows, schema=cols, orient="row")
    finally:
        cur.close()
