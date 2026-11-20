"""Tests for public scoring presets, fumbles_lost_total, K, DST, TEP, and PPFD."""

from pathlib import Path
import sys
import tempfile
import unittest

import duckdb
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.catalog import Dataset
from nfl_warehouse.paths import WarehousePaths
from nfl_warehouse.scoring import (
    build_player_week_points,
    build_site_scoring,
    build_team_defense_week_points,
    load_scoring_config,
)
from nfl_warehouse.views import create_views


class TestScoring(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.paths = WarehousePaths.for_root(self.tmpdir.name)

        psw_dir = self.paths.nflverse_dir / "player_stats_week"
        ts_dir = self.paths.nflverse_dir / "team_stats_week"
        sch_dir = self.paths.nflverse_dir / "schedules"
        psw_dir.mkdir(parents=True, exist_ok=True)
        ts_dir.mkdir(parents=True, exist_ok=True)
        sch_dir.mkdir(parents=True, exist_ok=True)

        # Players:
        # 1) WR1: 3 rec, 68 yds, 100-yd=no, 0 offensive fumbles lost, 1 fumbles_lost_total (punt return fumble)
        #    -> nflverse PPR: 3*1 + 6.8 = 9.8 ; Yahoo (0.5 PPR + fum_lost_total): 1.5 + 6.8 - 2.0 = 6.3
        # 2) TE1: 4 rec, 50 rec_yds, 2 rec_first_downs
        #    -> PPR: 9.0 ; TEP+PPFD example: 4*1 + 4*0.5 + 5.0 + 2*0.5 = 12.0
        # 3) K1: 2 FG 0-39, 1 FG 40-49, 1 FG 50+, 1 FG missed, 2 PAT made
        #    -> PPR/standard K: 2*3 + 1*4 + 1*5 - 1 + 2 = 16.0 ; Yahoo K (0 missed penalty): 17.0
        pl.DataFrame(
            {
                "player_id": ["WR1", "TE1", "K1"],
                "player_display_name": ["Punt Fumble WR", "Tight End One", "Kicker One"],
                "position": ["WR", "TE", "K"],
                "team": ["CHI", "KC", "BUF"],
                "season": [2025, 2025, 2025],
                "week": [1, 1, 1],
                "season_type": ["REG", "REG", "REG"],
                "passing_yards": [0.0, 0.0, 0.0],
                "passing_tds": [0.0, 0.0, 0.0],
                "passing_interceptions": [0.0, 0.0, 0.0],
                "passing_first_downs": [0.0, 0.0, 0.0],
                "passing_2pt_conversions": [0.0, 0.0, 0.0],
                "rushing_yards": [0.0, 0.0, 0.0],
                "rushing_tds": [0.0, 0.0, 0.0],
                "rushing_first_downs": [0.0, 0.0, 0.0],
                "rushing_fumbles_lost": [0.0, 0.0, 0.0],
                "rushing_2pt_conversions": [0.0, 0.0, 0.0],
                "receptions": [3.0, 4.0, 0.0],
                "receiving_yards": [68.0, 50.0, 0.0],
                "receiving_tds": [0.0, 0.0, 0.0],
                "receiving_first_downs": [2.0, 2.0, 0.0],
                "receiving_fumbles_lost": [0.0, 0.0, 0.0],
                "receiving_2pt_conversions": [0.0, 0.0, 0.0],
                "sack_fumbles_lost": [0.0, 0.0, 0.0],
                "fumbles_lost_total": [1.0, 0.0, 0.0],
                "special_teams_tds": [0.0, 0.0, 0.0],
                "fumble_recovery_tds": [0.0, 0.0, 0.0],
                "fg_made_0_19": [1.0, 0.0, 1.0],
                "fg_made_20_29": [0.0, 0.0, 1.0],
                "fg_made_30_39": [0.0, 0.0, 0.0],
                "fg_made_40_49": [0.0, 0.0, 1.0],
                "fg_made_50_59": [0.0, 0.0, 1.0],
                "fg_made_60_": [0.0, 0.0, 0.0],
                "fg_made": [0.0, 0.0, 4.0],
                "fg_missed": [0.0, 0.0, 1.0],
                "pat_made": [0.0, 0.0, 2.0],
                "pat_missed": [0.0, 0.0, 0.0],
            }
        ).write_parquet(psw_dir / "2025.parquet")

        # Team stats + schedule including historical STL->LA alignment and unplayed week 18
        pl.DataFrame(
            {
                "season": [2015, 2025, 2025],
                "week": [1, 1, 18],
                "team": ["LA", "SEA", None],
                "opponent_team": ["ARI", "SF", None],
                "season_type": ["REG", "REG", "REG"],
                "def_sacks": [3.0, 4.0, 0.0],
                "def_interceptions": [1.0, 2.0, 0.0],
                "fumble_recovery_opp": [1.0, 0.0, 0.0],
                "def_tds": [0.0, 1.0, 0.0],
                "special_teams_tds": [0.0, 0.0, 0.0],
                "def_safeties": [0.0, 1.0, 0.0],
                "def_punt_blocks": [0.0, 1.0, 0.0],
                "def_pat_blocks": [0.0, 0.0, 0.0],
                "def_fg_blocks": [0.0, 0.0, 0.0],
            }
        ).write_parquet(ts_dir / "2025.parquet")

        pl.DataFrame(
            {
                "game_id": ["2015_01_ARI_STL", "2025_01_SF_SEA", "2025_18_SF_SEA"],
                "season": [2015, 2025, 2025],
                "week": [1, 1, 18],
                "home_team": ["STL", "SEA", "SEA"],
                "away_team": ["ARI", "SF", "SF"],
                "home_score": [17, 24, None],
                "away_score": [10, 0, None],
            }
        ).write_parquet(sch_dir / "2025.parquet")

        self.con = duckdb.connect(str(self.paths.warehouse_path))
        create_views(
            self.con,
            self.paths,
            [
                Dataset("player_stats_week", "load_player_stats", seasonal=True),
                Dataset("team_stats_week", "load_team_stats", seasonal=True),
                Dataset("schedules", "load_schedules", seasonal=True),
            ],
            log=lambda _: None,
        )

    def tearDown(self):
        self.con.close()
        self.tmpdir.cleanup()

    def test_shipped_config_is_public_only(self):
        cfg = load_scoring_config(REPO_ROOT / "config" / "scoring.json")
        sites = {r["site"] for r in cfg["rulesets"]}
        self.assertEqual(sites, {"ppr", "half_ppr", "standard", "yahoo"})
        self.assertNotIn("mfl", sites)

    def test_fumbles_lost_total_kicker_and_dst_scoring(self):
        cfg = load_scoring_config(REPO_ROOT / "config" / "scoring.json")
        site_rows = build_site_scoring(self.con, cfg)
        self.assertGreater(site_rows, 0)
        p_rows = build_player_week_points(self.con, cfg, log=lambda _: None)
        self.assertEqual(p_rows, 12)  # 4 sites * (2 offense + 1 kicker)
        d_rows = build_team_defense_week_points(self.con, cfg, log=lambda _: None)
        self.assertEqual(d_rows, 8)  # 4 sites * 2 completed team rows (null team & unplayed excluded)

        wr_ppr = self.con.execute(
            "SELECT points FROM main.player_week_points WHERE player_id = 'WR1' AND site = 'ppr'"
        ).fetchone()[0]
        wr_yahoo = self.con.execute(
            "SELECT points FROM main.player_week_points WHERE player_id = 'WR1' AND site = 'yahoo'"
        ).fetchone()[0]
        self.assertAlmostEqual(wr_ppr, 9.8)
        self.assertAlmostEqual(wr_yahoo, 6.3)

        k_ppr = self.con.execute(
            "SELECT points FROM main.player_week_points WHERE player_id = 'K1' AND site = 'ppr'"
        ).fetchone()[0]
        k_yahoo = self.con.execute(
            "SELECT points FROM main.player_week_points WHERE player_id = 'K1' AND site = 'yahoo'"
        ).fetchone()[0]
        self.assertAlmostEqual(k_ppr, 16.0)
        self.assertAlmostEqual(k_yahoo, 17.0)

        # SEA 2025 W1: 0 PA (10) + 4 sacks (4) + 2 INT (4) + 1 def_td (6) + 1 safety (2) + 1 block (2) = 28
        sea_dst = self.con.execute(
            "SELECT points_allowed, points FROM main.team_defense_week_points WHERE team = 'SEA' AND site = 'ppr'"
        ).fetchone()
        self.assertEqual(sea_dst, (0, 28.0))

        # LA 2015 W1 matched STL in schedules: 10 PA (4) + 3 sacks (3) + 1 INT (2) + 1 fum (2) = 11
        la_dst = self.con.execute(
            "SELECT points_allowed, points FROM main.team_defense_week_points WHERE team = 'LA' AND site = 'ppr'"
        ).fetchone()
        self.assertEqual(la_dst, (10, 11.0))

    def test_custom_te_premium_ppfd_example_config(self):
        cfg = load_scoring_config(REPO_ROOT / "config" / "scoring_te_premium_ppfd.example.json")
        build_player_week_points(self.con, cfg, log=lambda _: None)
        te_pts = self.con.execute(
            "SELECT points FROM main.player_week_points WHERE player_id = 'TE1' AND site = 'ppr_tep_ppfd'"
        ).fetchone()[0]
        wr_pts = self.con.execute(
            "SELECT points FROM main.player_week_points WHERE player_id = 'WR1' AND site = 'ppr_tep_ppfd'"
        ).fetchone()[0]
        # TE1: 4 rec (4) + 4*0.5 TEP (2) + 50 yds (5) + 2 rec_fd*0.5 (1) = 12.0
        self.assertAlmostEqual(te_pts, 12.0)
        # WR1: 3 rec (3) + 68 yds (6.8) + 2 rec_fd*0.5 (1) - 1 fum_lost_total (2) = 8.8
        self.assertAlmostEqual(wr_pts, 8.8)


if __name__ == "__main__":
    unittest.main()
