"""Tests for DuckDB connect() read-only enforcement and generalized attachments."""

import hashlib
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

import duckdb

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.connect import connect, query_polars
from nfl_warehouse.paths import WarehousePaths


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestConnect(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.paths = WarehousePaths.for_root(self.tmp)
        self.paths.data_dir.mkdir(parents=True, exist_ok=True)

        self.sqlite_path = self.tmp / "external_league.db"
        scon = sqlite3.connect(self.sqlite_path)
        scon.execute("CREATE TABLE rosters (player_id TEXT)")
        scon.execute("INSERT INTO rosters VALUES ('p1')")
        scon.commit()
        scon.close()

        self.ext_duckdb_path = self.tmp / "external_other.duckdb"
        dcon = duckdb.connect(str(self.ext_duckdb_path))
        dcon.execute("CREATE TABLE custom_proj (gsis_id VARCHAR, proj DOUBLE)")
        dcon.execute("INSERT INTO custom_proj VALUES ('00-1', 18.5)")
        dcon.close()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_attach_sqlite_and_duckdb_read_only(self):
        before = (_sha256(self.sqlite_path), self.sqlite_path.stat().st_mtime_ns)

        # Initialize main warehouse first
        connect(self.paths).close()

        with connect(
            self.paths,
            read_only=True,
            attach={"league": self.sqlite_path, "proj": self.ext_duckdb_path},
        ) as con:
            self.assertEqual(con.execute("SELECT count(*) FROM league.rosters").fetchone()[0], 1)
            self.assertEqual(
                con.execute("SELECT proj FROM proj.custom_proj WHERE gsis_id = '00-1'").fetchone()[0],
                18.5,
            )
            with self.assertRaises(duckdb.Error):
                con.execute("INSERT INTO league.rosters VALUES ('x')")
            with self.assertRaises(duckdb.Error):
                con.execute("INSERT INTO proj.custom_proj VALUES ('00-2', 5.0)")
            with self.assertRaises(duckdb.Error):
                con.execute("CREATE TABLE main.t (x INT)")

        self.assertEqual(
            before,
            (_sha256(self.sqlite_path), self.sqlite_path.stat().st_mtime_ns),
        )

    def test_read_only_missing_warehouse_raises(self):
        with self.assertRaises(FileNotFoundError):
            connect(self.paths, read_only=True)

    def test_invalid_alias_rejected_and_missing_file_skipped(self):
        with self.assertRaises(ValueError):
            connect(self.paths, attach={"bad-alias": self.sqlite_path})

        logs: list[str] = []
        with connect(
            self.paths,
            attach={"missing_db": self.tmp / "does_not_exist.db"},
            log=logs.append,
        ) as con:
            self.assertEqual(con.execute("SELECT 1").fetchone()[0], 1)
        self.assertTrue(any("missing_db" in line for line in logs))

    def test_query_polars_supports_params_and_empty_results(self):
        with connect(self.paths, attach={"proj": self.ext_duckdb_path}) as con:
            pending = con.execute("SELECT 1")
            df = query_polars(con, "SELECT gsis_id, proj FROM proj.custom_proj")
            self.assertEqual(df.to_dicts(), [{"gsis_id": "00-1", "proj": 18.5}])
            self.assertEqual(pending.fetchone()[0], 1)

            param_df = query_polars(
                con,
                "SELECT gsis_id, proj FROM proj.custom_proj WHERE gsis_id = ?",
                ["00-1"],
            )
            self.assertEqual(param_df.shape, (1, 2))

            empty_df = query_polars(
                con,
                "SELECT gsis_id, proj FROM proj.custom_proj WHERE gsis_id = ?",
                ["missing"],
            )
            self.assertEqual(empty_df.shape, (0, 2))
            self.assertEqual(empty_df.columns, ["gsis_id", "proj"])

    def test_package_has_no_private_league_coupling(self):
        pkg_src = "".join(
            p.read_text(encoding="utf-8") for p in (SRC_DIR / "nfl_warehouse").glob("*.py")
        )
        for forbidden in ("LeagueDB", "sqlite3.connect", "40694", "yahoo_dfs", "betql"):
            self.assertNotIn(forbidden, pkg_src)


if __name__ == "__main__":
    unittest.main()
