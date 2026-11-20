"""Tests for WarehousePaths and nflverse dataset catalog/profiles."""

import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.catalog import CATALOG, VALID_PROFILES, get_datasets
from nfl_warehouse.paths import ENV_WAREHOUSE_DIR, WarehousePaths, default_scoring_config_path


class TestPathsAndCatalog(unittest.TestCase):
    def test_for_root_and_data_dir(self):
        root_paths = WarehousePaths.for_root(REPO_ROOT)
        self.assertEqual(root_paths.warehouse_path, REPO_ROOT / "data" / "nfl_stats.duckdb")
        self.assertEqual(root_paths.manifest_path, REPO_ROOT / "data" / "nflverse" / "_manifest.json")
        self.assertEqual(root_paths.config_dir, REPO_ROOT / "config")

        ext_data = REPO_ROOT / "custom_data"
        ext_paths = WarehousePaths.for_data_dir(ext_data)
        self.assertEqual(ext_paths.data_dir, ext_data.resolve())
        self.assertEqual(ext_paths.warehouse_path, ext_data.resolve() / "nfl_stats.duckdb")
        self.assertEqual(ext_paths.config_dir, REPO_ROOT / "config")

    def test_env_override_warehouse_dir(self):
        custom = REPO_ROOT / "tmp_env_warehouse"
        with patch.dict(os.environ, {ENV_WAREHOUSE_DIR: str(custom)}):
            paths = WarehousePaths.default()
        self.assertEqual(paths.data_dir, custom.resolve())
        self.assertEqual(paths.warehouse_path, custom.resolve() / "nfl_stats.duckdb")
        self.assertEqual(paths.config_dir, REPO_ROOT / "config")

    def test_non_editable_wheel_requires_env_and_uses_bundled_scoring(self):
        with patch("nfl_warehouse.paths._find_repo_root", return_value=None):
            with patch.dict(os.environ, {ENV_WAREHOUSE_DIR: ""}):
                with self.assertRaisesRegex(RuntimeError, "NFL_WAREHOUSE_DIR"):
                    WarehousePaths.default()
            ext = REPO_ROOT / "tmp_wheel_data"
            with patch.dict(os.environ, {ENV_WAREHOUSE_DIR: str(ext)}):
                paths = WarehousePaths.default()
            self.assertEqual(paths.data_dir, ext.resolve())
            scoring_path = default_scoring_config_path(paths)
            self.assertTrue(scoring_path.is_file())
            self.assertEqual(scoring_path.name, "scoring.json")

    def test_catalog_counts_and_seasonality_invariants(self):
        self.assertEqual(len(CATALOG), 25)
        for ds in CATALOG:
            self.assertEqual(ds.seasonal, ds.first_season is not None, ds.name)

    def test_profiles_and_exclude_pbp(self):
        self.assertEqual(VALID_PROFILES, ("lite", "core", "full"))

        lite_names = {d.name for d in get_datasets(profile="lite")}
        self.assertEqual(len(lite_names), 15)
        self.assertNotIn("pbp", lite_names)
        self.assertIn("team_stats_week", lite_names)
        self.assertIn("trades", lite_names)
        self.assertNotIn("pfr_adv_def", lite_names)

        core_names = {d.name for d in get_datasets(profile="core")}
        self.assertEqual(len(core_names), 16)
        self.assertIn("pbp", core_names)

        no_pbp_core = {d.name for d in get_datasets(profile="core", exclude_pbp=True)}
        self.assertEqual(no_pbp_core, lite_names)

        full_names = {d.name for d in get_datasets(profile="full")}
        self.assertEqual(len(full_names), 25)
        self.assertIn("pfr_adv_def", full_names)
        self.assertIn("participation", full_names)

        opt_names = {d.name for d in get_datasets(include_optional=True)}
        self.assertEqual(opt_names, full_names)

    def test_unknown_profile_or_dataset_raises(self):
        with self.assertRaisesRegex(ValueError, "Unknown profile"):
            get_datasets(profile="ultra")
        with self.assertRaisesRegex(ValueError, "bogus"):
            get_datasets(names=["bogus"])

    def test_gitignore_protects_binaries_and_secrets(self):
        text = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
        for pattern in ("data/*.duckdb", "data/*.duckdb.wal", "data/nflverse/", ".env"):
            self.assertIn(pattern, text)


if __name__ == "__main__":
    unittest.main()
