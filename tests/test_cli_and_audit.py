"""Tests for CLI hydration profiles, --scoring-config, telemetry, and audit_warehouse."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
SCRIPTS_DIR = REPO_ROOT / "scripts"
for path in (SRC_DIR, SCRIPTS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import audit_warehouse
from nfl_warehouse.catalog import CATALOG, Dataset
from nfl_warehouse.cli import parse_args, run
from nfl_warehouse.connect import connect
from nfl_warehouse.download import execute_tasks
from nfl_warehouse.paths import WarehousePaths
from nfl_warehouse.telemetry import get_peak_rss_mb


class FakeLiteFetcher:
    def __init__(self):
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
                    "home_team": ["SEA"],
                    "away_team": ["SF"],
                    "home_score": [24],
                    "away_score": [14],
                }
            )
        if ds.name == "player_stats_week":
            return pl.DataFrame(
                {
                    "player_id": ["00-1", "00-2"],
                    "player_display_name": ["QB One", "Kicker One"],
                    "position": ["QB", "K"],
                    "team": ["SEA", "SEA"],
                    "season": [2025, 2025],
                    "week": [1, 1],
                    "season_type": ["REG", "REG"],
                    "passing_yards": [250.0, 0.0],
                    "passing_tds": [2.0, 0.0],
                    "passing_interceptions": [1.0, 0.0],
                    "rushing_yards": [10.0, 0.0],
                    "rushing_tds": [0.0, 0.0],
                    "receptions": [0.0, 0.0],
                    "receiving_yards": [0.0, 0.0],
                    "receiving_tds": [0.0, 0.0],
                    "rushing_fumbles_lost": [0.0, 0.0],
                    "receiving_fumbles_lost": [0.0, 0.0],
                    "sack_fumbles_lost": [0.0, 0.0],
                    "fumbles_lost_total": [0.0, 0.0],
                    "passing_2pt_conversions": [0.0, 0.0],
                    "rushing_2pt_conversions": [0.0, 0.0],
                    "receiving_2pt_conversions": [0.0, 0.0],
                    "special_teams_tds": [0.0, 0.0],
                    "fg_made_0_19": [0.0, 1.0],
                    "fg_made_20_29": [0.0, 0.0],
                    "fg_made_30_39": [0.0, 0.0],
                    "fg_made_40_49": [0.0, 1.0],
                    "fg_made_50_59": [0.0, 0.0],
                    "fg_made_60_": [0.0, 0.0],
                    "fg_missed": [0.0, 0.0],
                    "pat_made": [0.0, 3.0],
                    "fantasy_points_ppr": [17.0, 0.0],
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
        if ds.name == "rosters_weekly":
            return pl.DataFrame(
                {
                    "season": [2025],
                    "week": [1],
                    "gsis_id": ["00-1"],
                    "team": ["SEA"],
                    "position": ["QB"],
                    "status": ["ACT"],
                    "game_type": ["REG"],
                }
            )
        if ds.name == "depth_charts":
            return pl.DataFrame(
                {
                    "dt": ["2025-09-07T07:12:47Z"],
                    "team": ["SEA"],
                    "gsis_id": ["00-1"],
                    "player_name": ["QB One"],
                    "pos_abb": ["QB"],
                    "pos_rank": [1],
                }
            )
        if ds.name.startswith("nextgen_"):
            return pl.DataFrame(
                {
                    "season": [2025, 2025],
                    "week": [0, 1],
                    "team_abbr": ["LAR", "LAR"],
                    "player_gsis_id": ["00-1", "00-1"],
                }
            )
        if ds.name == "players":
            return pl.DataFrame({"gsis_id": ["00-1", "00-2"], "display_name": ["QB One", "Kicker One"]})
        if ds.name == "ff_playerids":
            return pl.DataFrame(
                {
                    "gsis_id": ["00-1", "00-2"],
                    "db_season": [2025, 2025],
                    "mfl_id": [1001, 1002],
                    "yahoo_id": [2001, 2002],
                }
            )
        return pl.DataFrame(
            {
                "season": [2025] if ds.seasonal else [2025],
                "team": ["SEA"],
                "val": [1],
            }
        )


class TestCliAndAudit(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.paths = WarehousePaths.for_root(self.tmpdir.name)
        self.orig_sleep = execute_tasks.__kwdefaults__["sleep"]
        execute_tasks.__kwdefaults__["sleep"] = lambda _: None

    def tearDown(self):
        execute_tasks.__kwdefaults__["sleep"] = self.orig_sleep
        self.tmpdir.cleanup()

    def test_parse_args_supports_lite_and_overrides(self):
        args = parse_args(
            [
                "--profile",
                "lite",
                "--no-pbp",
                "--scoring-config",
                "custom.json",
                "--data-dir",
                "D:/data",
                "--telemetry-out",
                "t.json",
            ]
        )
        self.assertEqual(args.profile, "lite")
        self.assertTrue(args.no_pbp)
        self.assertEqual(args.scoring_config, "custom.json")
        self.assertEqual(args.data_dir, "D:/data")
        self.assertEqual(args.telemetry_out, "t.json")

    def test_lite_hydration_skips_pbp_and_passes_audit(self):
        # Constrain seasonal datasets to 2025 for fast fixture hydration across all 15 lite datasets
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
        fetcher = FakeLiteFetcher()
        telemetry_path = self.paths.root / "telemetry.json"

        with patch("nfl_warehouse.catalog.CATALOG", short_catalog):
            code = run(
                ["--profile", "lite", "--seasons", "2025", "--telemetry-out", str(telemetry_path)],
                paths=self.paths,
                fetch=fetcher,
                current_season=2025,
                log=lambda _: None,
            )
        self.assertEqual(code, 0)
        fetched_datasets = {ds for ds, _ in fetcher.calls}
        self.assertNotIn("pbp", fetched_datasets)
        self.assertIn("team_stats_week", fetched_datasets)
        self.assertIn("trades", fetched_datasets)
        self.assertTrue(telemetry_path.is_file())

        audit_code = audit_warehouse.main(
            [
                "--data-dir",
                str(self.paths.data_dir),
                "--profile",
                "lite",
                "--allow-fixture-floors",
                "--strict",
                "--telemetry-out",
                str(telemetry_path),
            ]
        )
        self.assertEqual(audit_code, 0)
        payload = json.loads(telemetry_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["audit"]["overall_status"], "PASS")
        self.assertGreater(payload["footprint"]["warehouse_db_bytes"], 0)
        self.assertGreater(get_peak_rss_mb(), 0.0)
        self.assertGreater(payload["peak_rss_mb"], 0.0)

        # Verify --rebuild with custom --scoring-config and --status
        with patch("nfl_warehouse.catalog.CATALOG", short_catalog):
            rebuild_code = run(
                [
                    "--rebuild",
                    "--scoring-config",
                    str(REPO_ROOT / "config" / "scoring_te_premium_ppfd.example.json"),
                ],
                paths=self.paths,
                current_season=2025,
                log=lambda _: None,
            )
            self.assertEqual(rebuild_code, 0)
            status_logs: list[str] = []
            status_code = run(["--status"], paths=self.paths, log=status_logs.append)
            self.assertEqual(status_code, 0)
            self.assertTrue(any("team_stats_week" in line for line in status_logs))

        with connect(self.paths) as con:
            sites = {
                r[0]
                for r in con.execute("SELECT DISTINCT site FROM main.player_week_points").fetchall()
            }
            self.assertEqual(sites, {"ppr_tep_ppfd"})
            # Inject a non-portable absolute view and verify audit catches it
            con.execute("CREATE OR REPLACE VIEW nfl.bad_view AS SELECT 'C:/fake/path' AS p")

        bad_report = audit_warehouse.run_audit(
            self.paths, profile="lite", enforce_full_floors=False
        )
        port_check = next(c for c in bad_report["checks"] if c["name"] == "view_portability")
        self.assertEqual(port_check["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
