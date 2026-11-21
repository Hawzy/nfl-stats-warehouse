"""Tests for portable relative DuckDB views across moved directories and CWDs."""

from contextlib import chdir
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

import polars as pl

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.catalog import Dataset
from nfl_warehouse.connect import connect
from nfl_warehouse.download import parquet_path
from nfl_warehouse.paths import WarehousePaths
from nfl_warehouse.views import create_views

SEASONAL = Dataset(name="player_stats_week", loader="load_player_stats", seasonal=True)
PLAYERS = Dataset(name="players", loader="load_players", seasonal=False)
MISSING = Dataset(name="missing", loader="load_missing", seasonal=True)


class TestViewsPortability(unittest.TestCase):
    def setUp(self):
        self.tmp1 = tempfile.TemporaryDirectory()
        self.tmp2 = tempfile.TemporaryDirectory()
        self.tmp3 = tempfile.TemporaryDirectory()
        self.paths = WarehousePaths.for_root(self.tmp1.name)

        seasonal_dir = self.paths.nflverse_dir / "player_stats_week"
        seasonal_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"season": [2019], "interceptions": [5]}).write_parquet(
            seasonal_dir / "2019.parquet"
        )
        pl.DataFrame({"season": [2020], "passing_interceptions": [3]}).write_parquet(
            seasonal_dir / "2020.parquet"
        )

        players_file = parquet_path(self.paths, PLAYERS, None)
        players_file.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"gsis_id": ["00-1"], "display_name": ["P1"]}).write_parquet(players_file)

    def tearDown(self):
        self.tmp1.cleanup()
        self.tmp2.cleanup()
        self.tmp3.cleanup()

    def test_union_by_name_and_stray_tmp_ignored(self):
        (self.paths.nflverse_dir / "player_stats_week" / "2021.parquet.tmp").write_bytes(b"bad")
        with connect(self.paths) as con:
            created = create_views(con, self.paths, [SEASONAL, PLAYERS, MISSING], log=lambda _: None)
            self.assertEqual(created, ["player_stats_week", "players"])
            cols = [r[0] for r in con.execute("DESCRIBE nfl.player_stats_week").fetchall()]
            self.assertIn("interceptions", cols)
            self.assertIn("passing_interceptions", cols)
            self.assertEqual(
                con.execute("SELECT count(*) FROM nfl.player_stats_week").fetchone()[0], 2
            )

    def test_views_contain_zero_absolute_paths_and_survive_directory_move(self):
        with connect(self.paths) as con:
            create_views(con, self.paths, [SEASONAL, PLAYERS], log=lambda _: None)
            view_rows = con.execute(
                "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = 'nfl'"
            ).fetchall()
            self.assertEqual(len(view_rows), 2)
            for name, sql in view_rows:
                self.assertNotRegex(sql, r"([A-Za-z]:[/\\]|\\\\|/Users/|/home/|/tmp/)", name)
                self.assertIn("read_parquet('nflverse/", sql)

        # Copy the entire data/ folder to a brand-new directory and delete the original
        moved_data = Path(self.tmp2.name) / "relocated_data"
        shutil.copytree(self.paths.data_dir, moved_data)
        shutil.rmtree(self.paths.data_dir)

        moved_paths = WarehousePaths.for_data_dir(moved_data)
        # Query read-only from a third completely unrelated working directory without rebuilding!
        with chdir(self.tmp3.name):
            with connect(moved_paths, read_only=True) as con:
                self.assertEqual(
                    con.execute("SELECT count(*) FROM nfl.player_stats_week").fetchone()[0], 2
                )
                self.assertEqual(
                    con.execute("SELECT count(*) FROM nfl.players").fetchone()[0], 1
                )
                self.assertEqual(
                    con.cursor().execute("SELECT count(*) FROM nfl.player_stats_week").fetchone()[0],
                    2,
                )


if __name__ == "__main__":
    unittest.main()
