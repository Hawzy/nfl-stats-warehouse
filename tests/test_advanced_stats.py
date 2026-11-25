"""Tests for Gold analytical tables (player_week_adv, player_week_xfp, team_week_dvp)."""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse import (  # noqa: E402
    WarehousePaths,
    build_advanced_stats_tables,
    build_player_week_adv,
    build_player_week_xfp,
    build_team_week_dvp,
    connect,
    impute_constrained_routes,
)
from nfl_warehouse.advanced_stats import (  # noqa: E402
    EMPIRICAL_FUMBLE_LOST_RATE,
    FROZEN_ROUTE_BETAS,
)
from nfl_warehouse.catalog import CATALOG, Dataset  # noqa: E402
from nfl_warehouse.cli import run  # noqa: E402
from nfl_warehouse.download import execute_tasks  # noqa: E402


def _py_expected_xfp(
    *,
    pos: str,
    rec_weight: float,
    int_weight: float,
    pass_yds_exp: float,
    pass_td_exp: float,
    pass_int_exp: float,
    rush_yds_exp: float,
    rush_td_exp: float,
    rush_attempts_exp: float,
    rec_yds_exp: float,
    receptions_exp: float,
    rec_td_exp: float,
    two_pt_exp: float,
    sacks_suffered: float,
) -> float:
    lam = EMPIRICAL_FUMBLE_LOST_RATE.get(pos, 0.005)
    touches = max(
        0.0,
        rush_attempts_exp + receptions_exp + (sacks_suffered if pos == "QB" else 0.0),
    )
    raw = (
        0.04 * pass_yds_exp
        + 4.0 * pass_td_exp
        + int_weight * pass_int_exp
        + 0.1 * rush_yds_exp
        + 6.0 * rush_td_exp
        + 0.1 * rec_yds_exp
        + rec_weight * receptions_exp
        + 6.0 * rec_td_exp
        + 2.0 * two_pt_exp
        - 2.0 * lam * touches
    )
    return max(0.0, raw)


def _build_rich_synthetic_db(*, include_duplicates_and_postseason: bool = True) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl")

    con.execute(
        """
        CREATE TABLE main.player_dim (
            gsis_id VARCHAR PRIMARY KEY,
            display_name VARCHAR,
            pfr_id VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO main.player_dim VALUES
        ('00-0036442', 'Joe Burrow', 'BurrJo01'),
        ('00-0033280', 'Christian McCaffrey', 'McCaCh01'),
        ('00-0038543', 'Jaxon Smith-Njigba', 'SmitJa06'),
        ('00-0037744', 'Trey McBride', 'McBrTr00'),
        ('00-0039999', 'Luther Burden III', 'BurdLu00')
        """
    )

    con.execute(
        """
        CREATE TABLE nfl.pbp (
            season BIGINT, week BIGINT, season_type VARCHAR, posteam VARCHAR, qb_dropback DOUBLE
        )
        """
    )
    pbp_rows: list[tuple[int, int, str, str, float]] = []
    for wk in (7, 8, 9):
        for tm in ("CIN", "SF", "SEA", "ARI", "CHI"):
            raw_tm = "LAR" if tm == "LA" else tm
            for _ in range(40):
                pbp_rows.append((2025, wk, "REG", raw_tm, 1.0))
    if include_duplicates_and_postseason:
        # POST / week 0 rows must never contaminate REG team dropbacks
        for _ in range(30):
            pbp_rows.append((2025, 19, "POST", "SEA", 1.0))
            pbp_rows.append((2025, 0, "REG", "SEA", 1.0))
    con.executemany("INSERT INTO nfl.pbp VALUES (?,?,?,?,?)", pbp_rows)

    con.execute(
        """
        CREATE TABLE nfl.snap_counts (
            season BIGINT, game_type VARCHAR, week BIGINT, pfr_player_id VARCHAR,
            team VARCHAR, offense_snaps DOUBLE, offense_pct DOUBLE
        )
        """
    )
    snap_rows = [
        (2025, "REG", 7, "SmitJa06", "SEA", 54.0, 0.80),
        (2025, "REG", 8, "SmitJa06", "SEA", 56.0, 0.85),
        (2025, "REG", 9, "SmitJa06", "SEA", 58.0, 0.90),
        (2025, "REG", 7, "BurdLu00", "CHI", 35.0, 0.50),
        (2025, "REG", 8, "BurdLu00", "CHI", 36.0, 0.50),
        (2025, "REG", 7, "McCaCh01", "SF", 55.0, 0.85),
        (2025, "REG", 8, "McCaCh01", "SF", 56.0, 0.85),
        (2025, "REG", 7, "McBrTr00", "ARI", 52.0, 0.88),
        (2025, "REG", 8, "McBrTr00", "ARI", 55.0, 0.90),
        (2025, "REG", 9, "McBrTr00", "ARI", 54.0, 0.92),
    ]
    if include_duplicates_and_postseason:
        snap_rows.append((2025, "REG", 9, "SmitJa06", "SEA", 10.0, 0.15))
        snap_rows.append((2025, "POST", 19, "SmitJa06", "SEA", 60.0, 0.95))
    con.executemany("INSERT INTO nfl.snap_counts VALUES (?,?,?,?,?,?,?)", snap_rows)

    con.execute(
        """
        CREATE TABLE nfl.player_stats_week (
            player_id VARCHAR, player_display_name VARCHAR, position VARCHAR,
            team VARCHAR, opponent_team VARCHAR, season BIGINT, week BIGINT, season_type VARCHAR,
            attempts DOUBLE, sacks_suffered DOUBLE, passing_yards DOUBLE, passing_tds DOUBLE,
            passing_interceptions DOUBLE, passing_epa DOUBLE, passing_cpoe DOUBLE, passing_air_yards DOUBLE,
            carries DOUBLE, rushing_yards DOUBLE, rushing_tds DOUBLE,
            targets DOUBLE, receptions DOUBLE, receiving_yards DOUBLE, receiving_tds DOUBLE,
            receiving_air_yards DOUBLE, target_share DOUBLE, air_yards_share DOUBLE, wopr DOUBLE,
            rushing_fumbles_lost DOUBLE, receiving_fumbles_lost DOUBLE, sack_fumbles_lost DOUBLE,
            passing_2pt_conversions DOUBLE, rushing_2pt_conversions DOUBLE, receiving_2pt_conversions DOUBLE
        )
        """
    )
    con.execute(
        """
        INSERT INTO nfl.player_stats_week VALUES
        ('00-0038543', 'Jaxon Smith-Njigba', 'WR', 'SEA', 'ARI', 2025, 7, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 8, 110, 1, 110, 0.35, 0.50, 0.88, 0, 0, 0, 0, 0, 0),
        ('00-0038543', 'Jaxon Smith-Njigba', 'WR', 'SEA', 'ARI', 2025, 8, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 9, 120, 1, 130, 0.38, 0.55, 0.96, 0, 0, 0, 0, 0, 0),
        ('00-0038543', 'Jaxon Smith-Njigba', 'WR', 'SEA', 'LAR', 2025, 9, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 7, 102, 0, 105, 0.36, 0.52, 0.90, 0, 0, 0, 0, 0, 0),
        ('00-0039999', 'Luther Burden III', 'WR', 'CHI', 'GB', 2025, 7, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 6, 90, 1, 64, 0.25, 0.20, 0.52, 0, 0, 0, 0, 0, 0),
        ('00-0039999', 'Luther Burden III', 'WR', 'CHI', 'GB', 2025, 8, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 7, 95, 0, 72, 0.25, 0.22, 0.53, 0, 0, 0, 0, 0, 0),
        ('00-0033280', 'Christian McCaffrey', 'RB', 'SF', 'LAR', 2025, 7, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 18, 95, 1, 7, 6, 55, 0, 14, 0.20, 0.08, 0.36, 0, 0, 0, 0, 0, 0),
        ('00-0033280', 'Christian McCaffrey', 'RB', 'SF', 'LAR', 2025, 8, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 20, 110, 1, 6, 5, 48, 0, 12, 0.18, 0.06, 0.31, 1, 0, 0, 0, 0, 0),
        ('00-0037744', 'Trey McBride', 'TE', 'ARI', 'SEA', 2025, 7, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 7, 78, 1, 72, 0.26, 0.24, 0.56, 0, 0, 0, 0, 0, 0),
        ('00-0037744', 'Trey McBride', 'TE', 'ARI', 'SEA', 2025, 8, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 8, 92, 0, 80, 0.28, 0.25, 0.60, 0, 0, 0, 0, 0, 0),
        ('00-0037744', 'Trey McBride', 'TE', 'ARI', 'SEA', 2025, 9, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 6, 68, 0, 62, 0.24, 0.21, 0.51, 0, 0, 0, 0, 0, 0),
        ('00-0036442', 'Joe Burrow', 'QB', 'CIN', 'CLE', 2025, 7, 'REG', 36, 2, 310, 2, 1, 12.0, 5.5, 300, 3, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('00-0036442', 'Joe Burrow', 'QB', 'CIN', 'CLE', 2025, 8, 'REG', 38, 3, 355, 3, 1, 14.0, 6.2, 340, 2, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0)
        """
    )
    if include_duplicates_and_postseason:
        con.execute(
            """
            INSERT INTO nfl.player_stats_week VALUES
            -- Duplicate lower-volume row for JSN Week 9
            ('00-0038543', 'Jaxon Smith-Njigba', 'WR', 'SEA', 'LAR', 2025, 9, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 5, 0, 5, 0.05, 0.05, 0.10, 0, 0, 0, 0, 0, 0),
            -- Postseason & Preseason & week 0 rows must be filtered out
            ('00-0038543', 'Jaxon Smith-Njigba', 'WR', 'SEA', 'DET', 2025, 19, 'POST', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 9, 140, 2, 120, 0.35, 0.50, 0.90, 0, 0, 0, 0, 0, 0),
            ('00-0038543', 'Jaxon Smith-Njigba', 'WR', 'SEA', 'KC', 2025, 1, 'PRE', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 3, 30, 0, 25, 0.20, 0.20, 0.40, 0, 0, 0, 0, 0, 0),
            ('00-0038543', 'Jaxon Smith-Njigba', 'WR', 'SEA', 'SF', 2025, 0, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 40, 30, 400, 3, 400, 0.30, 0.40, 0.70, 0, 0, 0, 0, 0, 0),
            -- Non-skill Kicker row must be excluded
            ('00-9999999', 'Jason Myers', 'K', 'SEA', 'ARI', 2025, 7, 'REG', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            """
        )

    con.execute(
        """
        CREATE TABLE nfl.ff_opportunity (
            season DOUBLE, week DOUBLE, player_id VARCHAR,
            pass_yards_gained_exp DOUBLE, pass_touchdown_exp DOUBLE, pass_interception_exp DOUBLE,
            rush_yards_gained_exp DOUBLE, rush_touchdown_exp DOUBLE, rush_attempt DOUBLE,
            rec_yards_gained_exp DOUBLE, receptions_exp DOUBLE, rec_touchdown_exp DOUBLE,
            pass_two_point_conv_exp DOUBLE, rec_two_point_conv_exp DOUBLE, rush_two_point_conv_exp DOUBLE,
            total_fantasy_points_exp DOUBLE
        )
        """
    )
    # Trey McBride Week 9 intentionally omitted to verify has_ff_opp=False fallback
    con.execute(
        """
        INSERT INTO nfl.ff_opportunity VALUES
        (2025, 7, '00-0038543', 0, 0, 0, 0, 0, 0, 98, 7.6, 0.65, 0, 0, 0, 19.0),
        (2025, 8, '00-0038543', 0, 0, 0, 0, 0, 0, 112, 8.2, 0.72, 0, 0, 0, 21.0),
        (2025, 9, '00-0038543', 0, 0, 0, 0, 0, 0, 94, 7.1, 0.55, 0, 0, 0, 18.5),
        (2025, 7, '00-0039999', 0, 0, 0, 0, 0, 0, 68, 5.4, 0.45, 0, 0, 0, 14.0),
        (2025, 8, '00-0039999', 0, 0, 0, 0, 0, 0, 74, 5.8, 0.48, 0, 0, 0, 15.0),
        (2025, 7, '00-0033280', 0, 0, 0, 82, 0.65, 18, 44, 4.8, 0.25, 0, 0, 0, 22.0),
        (2025, 8, '00-0033280', 0, 0, 0, 94, 0.78, 20, 40, 4.2, 0.22, 0, 0, 0, 23.0),
        (2025, 7, '00-0037744', 0, 0, 0, 0, 0, 0, 72, 6.2, 0.55, 0, 0, 0, 16.5),
        (2025, 8, '00-0037744', 0, 0, 0, 0, 0, 0, 82, 6.8, 0.60, 0, 0, 0, 18.2),
        (2025, 7, '00-0036442', 290, 2.1, 0.75, 14, 0.12, 3, 0, 0, 0, 0, 0, 0, 21.5),
        (2025, 8, '00-0036442', 340, 2.6, 0.85, 10, 0.10, 2, 0, 0, 0, 0.4, 0, 0, 23.0)
        """
    )
    if include_duplicates_and_postseason:
        con.execute(
            """
            INSERT INTO nfl.ff_opportunity VALUES
            -- Team-level null player_id row and lower duplicate must be ignored
            (2025, 7, NULL, 0, 0, 0, 0, 0, 0, 10, 0.5, 0.1, 0, 0, 0, 1.5),
            (2025, 9, '00-0038543', 0, 0, 0, 0, 0, 0, 10, 1.0, 0.05, 0, 0, 0, 2.0)
            """
        )

    con.execute(
        """
        CREATE TABLE nfl.nextgen_rushing (
            season BIGINT, season_type VARCHAR, week BIGINT,
            player_gsis_id VARCHAR, rush_attempts DOUBLE, rush_yards_over_expected DOUBLE
        )
        """
    )
    con.execute(
        """
        INSERT INTO nfl.nextgen_rushing VALUES
        (2025, 'REG', 7, '00-0033280', 18, 12.0),
        (2025, 'REG', 8, '00-0033280', 20, 15.0)
        """
    )
    if include_duplicates_and_postseason:
        # week=0 rollup and POST row must never leak
        con.execute(
            """
            INSERT INTO nfl.nextgen_rushing VALUES
            (2025, 'REG', 0, '00-0033280', 38, 27.0),
            (2025, 'POST', 19, '00-0033280', 22, 18.0)
            """
        )

    return con


class FakeWarehouseFetcher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int | None]] = []

    def __call__(self, ds: Dataset, season: int | None) -> pl.DataFrame:
        self.calls.append((ds.name, season))
        if ds.name == "schedules":
            return pl.DataFrame(
                {
                    "game_id": ["2025_01_SF_SEA"],
                    "season": [2025],
                    "week": [1],
                    "game_type": ["REG"],
                    "gameday": ["2025-09-07"],
                    "gametime": ["13:00"],
                    "home_team": ["SEA"],
                    "away_team": ["SF"],
                    "home_score": [24],
                    "away_score": [14],
                }
            )
        if ds.name == "player_stats_week":
            return pl.DataFrame(
                {
                    "player_id": ["00-1", "00-2", "00-1"],
                    "player_display_name": ["WR One", "RB One", "WR One"],
                    "position": ["WR", "RB", "WR"],
                    "team": ["SEA", "SF", "SEA"],
                    "opponent_team": ["SF", "SEA", "SF"],
                    "season": [2025, 2025, 2025],
                    "week": [1, 1, 19],
                    "season_type": ["REG", "REG", "POST"],
                    "attempts": [0.0, 0.0, 0.0],
                    "sacks_suffered": [0.0, 0.0, 0.0],
                    "passing_yards": [0.0, 0.0, 0.0],
                    "passing_tds": [0.0, 0.0, 0.0],
                    "passing_interceptions": [0.0, 0.0, 0.0],
                    "carries": [0.0, 15.0, 0.0],
                    "rushing_yards": [0.0, 72.0, 0.0],
                    "rushing_tds": [0.0, 1.0, 0.0],
                    "targets": [9.0, 4.0, 10.0],
                    "receptions": [7.0, 3.0, 8.0],
                    "receiving_yards": [98.0, 24.0, 110.0],
                    "receiving_tds": [1.0, 0.0, 1.0],
                    "receiving_air_yards": [81.0, 4.0, 90.0],
                    "target_share": [0.28, 0.12, 0.30],
                    "air_yards_share": [0.35, 0.03, 0.40],
                    "wopr": [0.66, 0.20, 0.73],
                    "rushing_fumbles_lost": [0.0, 0.0, 0.0],
                    "receiving_fumbles_lost": [0.0, 0.0, 0.0],
                    "sack_fumbles_lost": [0.0, 0.0, 0.0],
                    "fumbles_lost_total": [0.0, 0.0, 0.0],
                    "passing_2pt_conversions": [0.0, 0.0, 0.0],
                    "rushing_2pt_conversions": [0.0, 0.0, 0.0],
                    "receiving_2pt_conversions": [0.0, 0.0, 0.0],
                    "special_teams_tds": [0.0, 0.0, 0.0],
                    "fantasy_points_ppr": [22.8, 18.6, 25.0],
                }
            )
        if ds.name == "team_stats_week":
            return pl.DataFrame(
                {
                    "season": [2025],
                    "week": [1],
                    "team": ["SEA"],
                    "opponent_team": ["SF"],
                    "season_type": ["REG"],
                    "def_sacks": [3.0],
                    "def_interceptions": [1.0],
                    "fumble_recovery_opp": [0.0],
                    "def_tds": [0.0],
                    "special_teams_tds": [0.0],
                    "def_safeties": [0.0],
                    "def_punt_blocks": [0.0],
                    "def_pat_blocks": [0.0],
                    "def_fg_blocks": [0.0],
                }
            )
        if ds.name == "snap_counts":
            return pl.DataFrame(
                {
                    "season": [2025, 2025],
                    "game_type": ["REG", "REG"],
                    "week": [1, 1],
                    "pfr_player_id": ["PfrWr01", "PfrRb01"],
                    "team": ["SEA", "SF"],
                    "offense_snaps": [52.0, 44.0],
                    "offense_pct": [0.85, 0.72],
                }
            )
        if ds.name == "ff_opportunity":
            return pl.DataFrame(
                {
                    "season": [2025, 2025],
                    "week": [1, 1],
                    "player_id": ["00-1", "00-2"],
                    "pass_yards_gained_exp": [0.0, 0.0],
                    "pass_touchdown_exp": [0.0, 0.0],
                    "pass_interception_exp": [0.0, 0.0],
                    "rush_yards_gained_exp": [0.0, 68.0],
                    "rush_touchdown_exp": [0.0, 0.55],
                    "rush_attempt": [0.0, 15.0],
                    "rec_yards_gained_exp": [84.0, 22.0],
                    "receptions_exp": [6.1, 2.8],
                    "rec_touchdown_exp": [0.5, 0.15],
                    "pass_two_point_conv_exp": [0.0, 0.0],
                    "rec_two_point_conv_exp": [0.0, 0.0],
                    "rush_two_point_conv_exp": [0.0, 0.0],
                    "total_fantasy_points_exp": [17.5, 16.5],
                }
            )
        if ds.name == "players":
            return pl.DataFrame(
                {
                    "gsis_id": ["00-1", "00-2"],
                    "display_name": ["WR One", "RB One"],
                    "pfr_id": ["PfrWr01", "PfrRb01"],
                }
            )
        if ds.name == "ff_playerids":
            return pl.DataFrame(
                {
                    "gsis_id": ["00-1", "00-2"],
                    "db_season": [2025, 2025],
                    "mfl_id": [1001, 1002],
                }
            )
        if ds.name == "rosters_weekly":
            return pl.DataFrame(
                {
                    "season": [2025, 2025],
                    "week": [1, 1],
                    "gsis_id": ["00-1", "00-2"],
                    "team": ["SEA", "SF"],
                    "position": ["WR", "RB"],
                    "status": ["ACT", "ACT"],
                    "game_type": ["REG", "REG"],
                }
            )
        if ds.name == "depth_charts":
            return pl.DataFrame(
                {
                    "dt": ["2025-09-07T07:12:47Z"],
                    "team": ["SEA"],
                    "gsis_id": ["00-1"],
                    "player_name": ["WR One"],
                    "pos_abb": ["WR"],
                    "pos_rank": [1],
                    "pos_grp": ["3WR 1TE"],
                }
            )
        if ds.name.startswith("nextgen_"):
            return pl.DataFrame(
                {
                    "season": [2025],
                    "week": [1],
                    "team_abbr": ["SEA"],
                    "player_gsis_id": ["00-1"],
                }
            )
        return pl.DataFrame({"season": [2025], "team": ["SEA"], "val": [1]})


class TestGoldAdvancedStats(unittest.TestCase):
    def test_empty_typed_tables_when_sources_missing(self) -> None:
        con = duckdb.connect(":memory:")
        try:
            counts = build_advanced_stats_tables(con, log=lambda _: None)
            self.assertEqual(
                counts,
                {"player_week_adv": 0, "player_week_xfp": 0, "team_week_dvp": 0},
            )

            adv_desc = {
                row[0]: row[1]
                for row in con.execute("DESCRIBE main.player_week_adv").fetchall()
            }
            self.assertEqual(adv_desc["gsis_id"], "VARCHAR")
            self.assertEqual(adv_desc["season"], "INTEGER")
            self.assertEqual(adv_desc["constrained_routes"], "DOUBLE")
            self.assertEqual(adv_desc["yprr_constrained"], "DOUBLE")

            xfp_desc = {
                row[0]: row[1]
                for row in con.execute("DESCRIBE main.player_week_xfp").fetchall()
            }
            self.assertEqual(xfp_desc["has_ff_opp"], "BOOLEAN")
            self.assertEqual(xfp_desc["xfp_ppr"], "DOUBLE")
            self.assertEqual(xfp_desc["luck_delta_std"], "DOUBLE")
            for mfl_col in ("xfp_mfl", "actual_mfl", "luck_delta_mfl"):
                self.assertNotIn(mfl_col, xfp_desc)

            dvp_desc = {
                row[0]: row[1]
                for row in con.execute("DESCRIBE main.team_week_dvp").fetchall()
            }
            self.assertEqual(dvp_desc["player_count"], "INTEGER")
            self.assertEqual(dvp_desc["xfp_allowed_yahoo"], "DOUBLE")
            for mfl_col in ("pts_allowed_mfl", "xfp_allowed_mfl"):
                self.assertNotIn(mfl_col, dvp_desc)
        finally:
            con.close()

    def test_missing_optional_views_fallback_cleanly(self) -> None:
        con = duckdb.connect(":memory:")
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS nfl")
            con.execute(
                """
                CREATE TABLE nfl.player_stats_week AS
                SELECT
                    '00-1' AS player_id,
                    'Test WR' AS player_display_name,
                    'WR' AS position,
                    'LAR' AS team,
                    'JAC' AS opponent_team,
                    2025 AS season,
                    3 AS week,
                    'REG' AS season_type,
                    0.0 AS attempts,
                    0.0 AS carries,
                    8.0 AS targets,
                    6.0 AS receptions,
                    84.0 AS receiving_yards,
                    1.0 AS receiving_tds,
                    72.0 AS receiving_air_yards
                """
            )
            # pbp, snap_counts, ff_opportunity, nextgen_rushing, participation, ftn_charting all absent
            counts = build_advanced_stats_tables(con, log=lambda _: None)
            self.assertEqual(
                counts,
                {"player_week_adv": 1, "player_week_xfp": 1, "team_week_dvp": 1},
            )
            adv_row = con.execute(
                """
                SELECT team, opponent_team, team_dropbacks, eff_dropbacks,
                       offense_snaps, snap_pct, est_routes, constrained_routes, ff_xfp_raw
                FROM main.player_week_adv
                """
            ).fetchone()
            self.assertEqual(adv_row[0], "LA")
            self.assertEqual(adv_row[1], "JAX")
            self.assertEqual(adv_row[2], 0.0)
            self.assertEqual(adv_row[3], 34.0)
            self.assertEqual(adv_row[4], 0.0)
            self.assertEqual(adv_row[5], 0.0)
            self.assertEqual(adv_row[6], 0.0)
            self.assertEqual(adv_row[7], 8.0)  # targets floor when snap_counts missing
            self.assertEqual(adv_row[8], 0.0)

            xfp_row = con.execute(
                """
                SELECT has_ff_opp, xfp_ppr, actual_ppr, xfp_half, actual_half,
                       xfp_std, actual_std, xfp_yahoo, actual_yahoo, luck_delta_ppr
                FROM main.player_week_xfp
                """
            ).fetchone()
            self.assertFalse(xfp_row[0])
            self.assertAlmostEqual(xfp_row[1], 20.4, places=6)
            self.assertAlmostEqual(xfp_row[2], 20.4, places=6)
            self.assertAlmostEqual(xfp_row[3], 17.4, places=6)
            self.assertAlmostEqual(xfp_row[5], 14.4, places=6)
            self.assertAlmostEqual(xfp_row[7], 17.4, places=6)
            self.assertAlmostEqual(xfp_row[9], 0.0, places=6)

            # Column types stay DOUBLE / BOOLEAN even without optional views
            xfp_types = {
                r[0]: r[1] for r in con.execute("DESCRIBE main.player_week_xfp").fetchall()
            }
            self.assertEqual(xfp_types["pass_yds_exp"], "DOUBLE")
            self.assertEqual(xfp_types["has_ff_opp"], "BOOLEAN")
        finally:
            con.close()

    def test_reg_grain_dedup_sql_python_parity_and_dvp(self) -> None:
        con = _build_rich_synthetic_db(include_duplicates_and_postseason=True)
        try:
            counts = build_advanced_stats_tables(con, log=lambda _: None)
            self.assertEqual(
                counts,
                {"player_week_adv": 12, "player_week_xfp": 12, "team_week_dvp": 12},
            )

            # Strict 1:1 REG grain on (gsis_id, season, week)
            dup_adv = con.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT (gsis_id, season, week)) FROM main.player_week_adv"
            ).fetchone()[0]
            dup_xfp = con.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT (gsis_id, season, week)) FROM main.player_week_xfp"
            ).fetchone()[0]
            self.assertEqual(dup_adv, 0)
            self.assertEqual(dup_xfp, 0)

            # Week 0 and Week 19 postseason never appear
            bad_wks = con.execute(
                "SELECT COUNT(*) FROM main.player_week_adv WHERE week < 1 OR week > 18"
            ).fetchone()[0]
            self.assertEqual(bad_wks, 0)

            # Non-skill K excluded
            k_cnt = con.execute(
                "SELECT COUNT(*) FROM main.player_week_adv WHERE position = 'K'"
            ).fetchone()[0]
            self.assertEqual(k_cnt, 0)

            # Constrained routes SQL vs Python parity on every player-week
            adv_rows = con.execute(
                """
                SELECT gsis_id, position, eff_dropbacks, offense_snaps, snap_pct,
                       carries, team_carries, targets, receiving_air_yards, constrained_routes
                FROM main.player_week_adv
                """
            ).fetchall()
            self.assertEqual(len(FROZEN_ROUTE_BETAS), 3)
            for _gid, pos, edb, osn, spct, car, tcar, tgt, r_air, sql_routes in adv_rows:
                adot_p = (r_air / tgt) if tgt > 0 else (10.5 if pos == "WR" else 7.0)
                py_routes = (
                    impute_constrained_routes(pos, edb, osn, spct, car, tcar, tgt, adot_p, 0.22)
                    if pos in ("WR", "TE", "RB") and osn > 0
                    else float(tgt)
                )
                self.assertAlmostEqual(sql_routes, py_routes, places=6)

            # xFP SQL vs Python parity across all 4 league-agnostic presets
            xfp_rows = con.execute(
                """
                SELECT position, has_ff_opp,
                       pass_yds_exp, pass_td_exp, pass_int_exp,
                       rush_yds_exp, rush_td_exp, rush_attempts_exp,
                       rec_yds_exp, receptions_exp, rec_td_exp, two_pt_exp, sacks_suffered,
                       xfp_ppr, xfp_half, xfp_std, xfp_yahoo,
                       actual_ppr, actual_half, actual_std, actual_yahoo,
                       luck_delta_ppr, luck_delta_half, luck_delta_std, luck_delta_yahoo
                FROM main.player_week_xfp
                """
            ).fetchall()
            for row in xfp_rows:
                pos, has_opp = row[0], bool(row[1])
                base_kw = dict(
                    pos=pos,
                    pass_yds_exp=row[2],
                    pass_td_exp=row[3],
                    pass_int_exp=row[4],
                    rush_yds_exp=row[5],
                    rush_td_exp=row[6],
                    rush_attempts_exp=row[7],
                    rec_yds_exp=row[8],
                    receptions_exp=row[9],
                    rec_td_exp=row[10],
                    two_pt_exp=row[11],
                    sacks_suffered=row[12],
                )
                if has_opp:
                    self.assertAlmostEqual(
                        row[13], _py_expected_xfp(rec_weight=1.0, int_weight=-2.0, **base_kw), places=6
                    )
                    self.assertAlmostEqual(
                        row[14], _py_expected_xfp(rec_weight=0.5, int_weight=-2.0, **base_kw), places=6
                    )
                    self.assertAlmostEqual(
                        row[15], _py_expected_xfp(rec_weight=0.0, int_weight=-2.0, **base_kw), places=6
                    )
                    self.assertAlmostEqual(
                        row[16], _py_expected_xfp(rec_weight=0.5, int_weight=-1.0, **base_kw), places=6
                    )
                else:
                    self.assertAlmostEqual(row[13], max(0.0, row[17]), places=6)
                    self.assertAlmostEqual(row[14], max(0.0, row[18]), places=6)
                    self.assertAlmostEqual(row[15], max(0.0, row[19]), places=6)
                    self.assertAlmostEqual(row[16], max(0.0, row[20]), places=6)

                # luck_delta = actual - xfp
                self.assertAlmostEqual(row[21], row[17] - row[13], places=6)
                self.assertAlmostEqual(row[22], row[18] - row[14], places=6)
                self.assertAlmostEqual(row[23], row[19] - row[15], places=6)
                self.assertAlmostEqual(row[24], row[20] - row[16], places=6)

            # DvP aggregates match player sums by normalized defense_team (LAR -> LA)
            dvp_la_rb = con.execute(
                """
                SELECT player_count, pts_allowed_ppr, xfp_allowed_ppr
                FROM main.team_week_dvp
                WHERE defense_team = 'LA' AND position = 'RB' AND season = 2025 AND week = 7
                """
            ).fetchone()
            self.assertIsNotNone(dvp_la_rb)
            self.assertEqual(dvp_la_rb[0], 1)
            self.assertAlmostEqual(dvp_la_rb[1], 27.0, places=5)  # 95*0.1 + 6 + 6*1 + 55*0.1
        finally:
            con.close()

    def test_cli_build_and_idempotent_refresh_rebuild_force(self) -> None:
        short_catalog = tuple(
            Dataset(
                name=d.name,
                loader=d.loader,
                loader_kwargs=d.loader_kwargs,
                seasonal=d.seasonal,
                first_season=2025 if d.seasonal else None,
                optional=d.optional,
                heavy=d.heavy,
            )
            for d in CATALOG
        )
        fetcher = FakeWarehouseFetcher()
        orig_sleep = execute_tasks.__kwdefaults__["sleep"]
        execute_tasks.__kwdefaults__["sleep"] = lambda _: None
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                paths = WarehousePaths.for_root(tmpdir)
                with patch("nfl_warehouse.catalog.CATALOG", short_catalog):
                    rc_init = run(
                        ["--profile", "lite", "--seasons", "2025"],
                        paths=paths,
                        fetch=fetcher,
                        current_season=2025,
                        log=lambda _: None,
                    )
                    self.assertEqual(rc_init, 0)

                    with connect(paths, read_only=True) as con:
                        init_adv = con.execute(
                            "SELECT gsis_id, offense_snaps, snap_pct, constrained_routes "
                            "FROM main.player_week_adv ORDER BY gsis_id"
                        ).fetchall()
                        init_xfp = con.execute(
                            "SELECT gsis_id, has_ff_opp, xfp_ppr, actual_ppr "
                            "FROM main.player_week_xfp ORDER BY gsis_id"
                        ).fetchall()
                        init_dvp = con.execute(
                            "SELECT defense_team, position, player_count, pts_allowed_ppr "
                            "FROM main.team_week_dvp ORDER BY defense_team, position"
                        ).fetchall()

                    # snap_counts bridged via main.player_dim on very first build!
                    self.assertEqual(len(init_adv), 2)
                    self.assertEqual(init_adv[0][1], 52.0)
                    self.assertEqual(init_adv[0][2], 0.85)
                    self.assertTrue(all(row[1] for row in init_xfp))
                    self.assertEqual(len(init_dvp), 2)

                    for argv in (
                        ["--refresh", "--profile", "lite", "--seasons", "2025"],
                        ["--rebuild"],
                        ["--force", "--datasets", "player_stats_week", "--seasons", "2025"],
                    ):
                        rc = run(
                            argv,
                            paths=paths,
                            fetch=fetcher,
                            current_season=2025,
                            log=lambda _: None,
                        )
                        self.assertEqual(rc, 0)
                        with connect(paths, read_only=True) as con:
                            self.assertEqual(
                                con.execute(
                                    "SELECT gsis_id, offense_snaps, snap_pct, constrained_routes "
                                    "FROM main.player_week_adv ORDER BY gsis_id"
                                ).fetchall(),
                                init_adv,
                            )
                            self.assertEqual(
                                con.execute(
                                    "SELECT gsis_id, has_ff_opp, xfp_ppr, actual_ppr "
                                    "FROM main.player_week_xfp ORDER BY gsis_id"
                                ).fetchall(),
                                init_xfp,
                            )
                            self.assertEqual(
                                con.execute(
                                    "SELECT defense_team, position, player_count, pts_allowed_ppr "
                                    "FROM main.team_week_dvp ORDER BY defense_team, position"
                                ).fetchall(),
                                init_dvp,
                            )
        finally:
            execute_tasks.__kwdefaults__["sleep"] = orig_sleep

    def test_individual_builders_handle_missing_upstream_cleanly(self) -> None:
        con = duckdb.connect(":memory:")
        try:
            self.assertEqual(build_player_week_adv(con, log=lambda _: None), 0)
            self.assertEqual(build_player_week_xfp(con, log=lambda _: None), 0)
            # Drop xfp to verify build_team_week_dvp standalone handles missing xfp table
            con.execute("DROP TABLE main.player_week_xfp")
            self.assertEqual(build_team_week_dvp(con, log=lambda _: None), 0)
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
