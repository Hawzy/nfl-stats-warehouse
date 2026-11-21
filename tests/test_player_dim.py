"""Tests for public main.player_dim crosswalk build."""

from pathlib import Path
import sys
import tempfile
import unittest

import duckdb
import polars as pl

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.catalog import Dataset
from nfl_warehouse.paths import WarehousePaths
from nfl_warehouse.player_dim import PlayerDimResult, build_player_dim
from nfl_warehouse.views import create_views

PLAYERS_DS = Dataset(name="players", loader="load_players", seasonal=False)
FF_DS = Dataset(name="ff_playerids", loader="load_ff_playerids", seasonal=False)


class TestPlayerDim(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.paths = WarehousePaths.for_root(self.tmpdir.name)
        p_dir = self.paths.nflverse_dir / "players"
        ff_dir = self.paths.nflverse_dir / "ff_playerids"
        p_dir.mkdir(parents=True, exist_ok=True)
        ff_dir.mkdir(parents=True, exist_ok=True)

        pl.DataFrame(
            {
                "gsis_id": ["00-0034796", "00-0036212", "00-0031235", "00-0034827"],
                "display_name": ["Lamar Jackson", "Tua Tagovailoa", "Odell Beckham Jr.", "D.J. Moore"],
                "position": ["QB", "QB", "WR", "WR"],
                "birth_date": ["1997-01-07", "1998-03-02", "1992-11-05", "1997-04-14"],
                "height": [74.0, 73.0, 71.0, 72.0],
                "weight": [215.0, 217.0, 198.0, 210.0],
                "college_name": ["Louisville", "Alabama", "LSU", "Maryland"],
                "draft_year": [2018, 2020, 2014, 2018],
                "draft_round": [1, 1, 1, 1],
                "draft_pick": [32, 5, 12, 24],
                "pfr_id": ["JackLa00", "TagoTu00", None, "MoorD.00"],
                "espn_id": ["3916387", "4241479", None, None],
                "pff_id": ["46416", "55555", None, None],
                "latest_team": ["BAL", "MIA", "MIA", "CHI"],
                "last_season": [2026, 2026, 2025, 2026],
            }
        ).write_parquet(p_dir / "latest.parquet")

        pl.DataFrame(
            {
                "gsis_id": ["00-0034796", "00-0034796", "00-0036212", "00-0034827", "00-0041004", None],
                "db_season": [2022, 2026, 2026, 2026, 2026, 2026],
                "mfl_id": [None, 13589.0, 14056.0, 13640.0, 17522.0, 99999.0],
                "yahoo_id": [None, 30977.0, None, None, None, None],
                "fantasypros_id": [None, 17233.0, None, None, None, None],
                "sleeper_id": ["1111", "4881", None, None, None, None],
                "espn_id": [None, "3916387", "9999999", None, None, None],
                "pfr_id": [None, "JackLa00_ff", None, "MoorD.00", None, "Anon00"],
                "sportradar_id": [None, "fb52b041-0000-0000-0000-000000000000", None, None, None, None],
                "rotowire_id": [None, 12553.0, None, None, None, None],
                "pff_id": [None, None, "88888", None, None, None],
                "name": [None, None, None, None, "Rookie Wideout", "Ghost Player"],
                "position": [None, None, None, None, "WR", "RB"],
                "draft_year": [None, None, None, None, 2026, 2026],
                "draft_round": [None, None, None, None, 0, None],
                "merge_name": [None, "lamar jackson", "tua tagovailoa", "d.j. moore", None, "ghost player"],
            }
        ).write_parquet(ff_dir / "latest.parquet")

        self.con = duckdb.connect(str(self.paths.warehouse_path))
        create_views(self.con, self.paths, [PLAYERS_DS, FF_DS], log=lambda _: None)
        self.result = build_player_dim(self.con, log=lambda _: None)

    def tearDown(self):
        self.con.close()
        self.tmpdir.cleanup()

    def _row(self, gsis_id: str) -> dict:
        cur = self.con.execute("SELECT * FROM main.player_dim WHERE gsis_id = ?", [gsis_id])
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, cur.fetchone()))

    def test_crosswalk_ids_normalized_and_joined(self):
        lamar = self._row("00-0034796")
        self.assertEqual(lamar["mfl_id"], "13589")
        self.assertEqual(lamar["yahoo_id"], "30977")
        self.assertEqual(lamar["fantasypros_id"], "17233")
        self.assertEqual(lamar["sleeper_id"], "4881")
        self.assertEqual(lamar["rotowire_id"], "12553")
        self.assertEqual(lamar["pff_id"], "46416")
        self.assertEqual(lamar["pfr_id"], "JackLa00")
        tua = self._row("00-0036212")
        self.assertEqual(tua["mfl_id"], "14056")
        self.assertEqual(tua["espn_id"], "4241479")
        self.assertEqual(tua["pff_id"], "55555")
        moore = self._row("00-0034827")
        self.assertEqual(moore["pfr_id"], "MoorD.00")
        self.assertEqual(moore["mfl_id"], "13640")

    def test_no_latest_team_leak_and_dropped_count(self):
        cols = {r[0].lower() for r in self.con.execute("DESCRIBE main.player_dim").fetchall()}
        self.assertNotIn("latest_team", cols)
        self.assertEqual(self.result, PlayerDimResult(rows=5, dropped_no_gsis=1))

    def test_ff_playerids_only_row_preserved_via_outer_join(self):
        rookie = self._row("00-0041004")
        self.assertEqual(rookie["display_name"], "Rookie Wideout")
        self.assertEqual(rookie["merge_name"], "rookie wideout")
        self.assertEqual(rookie["position"], "WR")
        self.assertEqual(rookie["draft_year"], 2026)
        self.assertIsNone(rookie["draft_round"])
        self.assertEqual(rookie["mfl_id"], "17522")

    def test_merge_name_fallback_and_missing_views(self):
        self.assertEqual(self._row("00-0031235")["merge_name"], "odell beckham")
        empty = duckdb.connect(":memory:")
        try:
            self.assertEqual(build_player_dim(empty, log=lambda _: None), PlayerDimResult(0, 0))
        finally:
            empty.close()


if __name__ == "__main__":
    unittest.main()
