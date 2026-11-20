"""Tests for mart.* point-in-time backtesting guardrail views."""

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
from nfl_warehouse.mart import build_mart_views
from nfl_warehouse.paths import WarehousePaths
from nfl_warehouse.views import create_views


class TestMartGuardrails(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.paths = WarehousePaths.for_root(self.tmpdir.name)

        for folder in (
            "nextgen_passing",
            "nextgen_rushing",
            "nextgen_receiving",
            "rosters_weekly",
            "depth_charts",
            "schedules",
        ):
            (self.paths.nflverse_dir / folder).mkdir(parents=True, exist_ok=True)

        # NextGen: includes week=0 rollup + LAR abbreviation
        for ng_name in ("nextgen_passing", "nextgen_rushing", "nextgen_receiving"):
            pl.DataFrame(
                {
                    "season": [2025, 2025],
                    "week": [0, 1],
                    "team_abbr": ["LAR", "LAR"],
                    "player_gsis_id": ["00-1", "00-1"],
                    "player_display_name": ["Matthew Stafford", "Matthew Stafford"],
                }
            ).write_parquet(self.paths.nflverse_dir / ng_name / "2025.parquet")

        # Rosters weekly: duplicate gsis_id (ACT vs INA) + historical ARZ abbreviation
        pl.DataFrame(
            {
                "season": [2012, 2012],
                "week": [1, 1],
                "gsis_id": ["00-2", "00-2"],
                "team": ["ARZ", "SEA"],
                "position": ["WR", "WR"],
                "depth_chart_position": ["WR", "WR"],
                "full_name": ["Test Receiver", "Test Receiver"],
                "status": ["ACT", "INA"],
                "game_type": ["REG", "REG"],
                "jersey_number": ["11", "11"],
            }
        ).write_parquet(self.paths.nflverse_dir / "rosters_weekly" / "2012.parquet")

        # Schedules for 2025 W1 (LAR plays 2025-09-07 13:00 ET) + 2026 W5 London (JAX plays 2026-10-11 09:30 ET)
        pl.DataFrame(
            {
                "game_id": ["2025_01_HOU_LA", "2026_05_CHI_JAX"],
                "season": [2025, 2026],
                "week": [1, 5],
                "game_type": ["REG", "REG"],
                "gameday": ["2025-09-07", "2026-10-11"],
                "gametime": ["13:00", "09:30"],
                "home_team": ["LA", "JAX"],
                "away_team": ["HOU", "CHI"],
            }
        ).write_parquet(self.paths.nflverse_dir / "schedules" / "2025.parquet")

        # Depth charts split: 2024 legacy season/week + 2025/2026 dt snapshots (pre-kickoff & post-kickoff)
        pl.DataFrame(
            {
                "season": [2024],
                "week": [1],
                "club_code": ["LAR"],
                "game_type": ["REG"],
                "gsis_id": ["00-1"],
                "full_name": ["Matthew Stafford"],
                "position": ["QB"],
                "depth_team": ["1"],
                "formation": ["Offense"],
            }
        ).write_parquet(self.paths.nflverse_dir / "depth_charts" / "2024.parquet")

        pl.DataFrame(
            {
                "dt": [
                    "2025-09-05T07:12:47Z",  # older pre-game
                    "2025-09-07T07:12:47Z",  # latest pre-game (must be selected!)
                    "2025-09-08T07:12:47Z",  # post-game lookahead (must be excluded!)
                    "2026-10-11T06:01:52Z",  # London 09:30 ET (= 13:30 UTC) pre-kickoff (must be selected!)
                    "2026-10-11T13:56:31Z",  # London +26.5 min same-day post-kickoff leak (must be excluded!)
                ],
                "team": ["LA", "LA", "LA", "JAX", "JAX"],
                "gsis_id": ["00-1", "00-1", "00-1", "00-3", "00-3"],
                "player_name": [
                    "Old Name",
                    "PreGame Stafford",
                    "PostGame Leak",
                    "PreKickoff Lawrence",
                    "PostKickoff London Leak",
                ],
                "pos_abb": ["QB", "QB", "QB", "QB", "QB"],
                "pos_rank": [2, 1, 9, 1, 9],
                "pos_grp": ["Offense", "Offense", "Offense", "Offense", "Offense"],
            }
        ).write_parquet(self.paths.nflverse_dir / "depth_charts" / "2025.parquet")

        self.con = duckdb.connect(str(self.paths.warehouse_path))
        create_views(
            self.con,
            self.paths,
            [
                Dataset("nextgen_passing", "load_nextgen_stats", seasonal=True),
                Dataset("nextgen_rushing", "load_nextgen_stats", seasonal=True),
                Dataset("nextgen_receiving", "load_nextgen_stats", seasonal=True),
                Dataset("rosters_weekly", "load_rosters_weekly", seasonal=True),
                Dataset("depth_charts", "load_depth_charts", seasonal=True),
                Dataset("schedules", "load_schedules", seasonal=True),
            ],
            log=lambda _: None,
        )
        build_mart_views(self.con, log=lambda _: None)

    def tearDown(self):
        self.con.close()
        self.tmpdir.cleanup()

    def test_nextgen_excludes_week_zero_and_normalizes_lar(self):
        for view in ("nextgen_passing_weekly", "nextgen_rushing_weekly", "nextgen_receiving_weekly"):
            rows = self.con.execute(f"SELECT week, team FROM mart.{view}").fetchall()
            self.assertEqual(rows, [(1, "LA")])

    def test_player_team_week_deduplicates_and_normalizes(self):
        rows = self.con.execute(
            "SELECT season, week, gsis_id, team, raw_team, status FROM mart.player_team_week"
        ).fetchall()
        self.assertEqual(rows, [(2012, 1, "00-2", "ARI", "ARZ", "ACT")])

    def test_depth_charts_weekly_bridges_eras_without_postgame_leakage(self):
        rows = self.con.execute(
            "SELECT season, week, team, player_name, depth_rank, source_format "
            "FROM mart.depth_charts_weekly ORDER BY season, week"
        ).fetchall()
        self.assertEqual(
            rows,
            [
                (2024, 1, "LA", "Matthew Stafford", 1, "legacy_weekly"),
                (2025, 1, "LA", "PreGame Stafford", 1, "dt_pregame"),
                (2026, 5, "JAX", "PreKickoff Lawrence", 1, "dt_pregame"),
            ],
        )

    def test_depth_charts_london_kickoff_cutoff(self):
        rows = self.con.execute(
            "SELECT player_name, depth_rank, (snapshot_dt AT TIME ZONE 'UTC')::TIMESTAMP "
            "FROM mart.depth_charts_weekly WHERE season = 2026 AND week = 5 AND team = 'JAX'"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "PreKickoff Lawrence")
        self.assertEqual(rows[0][1], 1)
        self.assertEqual(rows[0][2].isoformat(), "2026-10-11T06:01:52")

    def test_no_mart_view_references_participation(self):
        views = self.con.execute(
            "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = 'mart'"
        ).fetchall()
        for name, sql in views:
            self.assertNotIn("participation", (sql or "").lower(), name)


if __name__ == "__main__":
    unittest.main()
