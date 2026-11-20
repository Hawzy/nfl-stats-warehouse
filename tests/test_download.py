"""Tests for atomic Parquet writes, retries, and DatasetNotAvailable handling."""

from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import polars as pl

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.catalog import Dataset
from nfl_warehouse.download import (
    DatasetNotAvailable,
    execute_tasks,
    nflreadpy_fetch,
    parquet_path,
)
from nfl_warehouse.lifecycle import DownloadTask
from nfl_warehouse.manifest import Manifest
from nfl_warehouse.paths import WarehousePaths


def _task(name: str, season: int | None, reason: str = "missing") -> DownloadTask:
    ds = Dataset(name=name, loader=f"load_{name}", seasonal=(season is not None))
    return DownloadTask(dataset=ds, season=season, reason=reason)


def _noop(*args, **kwargs) -> None:
    pass


class TestDownload(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.paths = WarehousePaths.for_root(self.tmpdir.name)
        self.paths.nflverse_dir.mkdir(parents=True, exist_ok=True)
        self.manifest = Manifest(path=self.paths.manifest_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_writes_parquet_and_manifest_atomically(self):
        fixed = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
        summary = execute_tasks(
            [_task("schedules", 2025)],
            self.paths,
            self.manifest,
            fetch=lambda ds, s: pl.DataFrame({"season": [s], "x": [1]}),
            sleep=_noop,
            log=_noop,
            now=lambda: fixed,
        )
        self.assertEqual(summary.written, [("schedules", 2025)])
        target = self.paths.nflverse_dir / "schedules" / "2025.parquet"
        self.assertTrue(target.is_file())
        self.assertEqual(list(target.parent.glob("*.tmp")), [])
        entry = Manifest.load(self.paths.manifest_path).get("schedules", 2025)
        self.assertEqual(entry.rows, 1)
        self.assertEqual(entry.downloaded_at, fixed.isoformat())

    def test_failed_write_leaves_no_partial_file(self):
        def boom(ds: Dataset, s: int | None) -> pl.DataFrame:
            raise RuntimeError("network error")

        summary = execute_tasks(
            [_task("schedules", 2025)],
            self.paths,
            self.manifest,
            fetch=boom,
            sleep=_noop,
            log=_noop,
            max_retries=2,
        )
        self.assertEqual(len(summary.failed), 1)
        self.assertEqual(list(self.paths.nflverse_dir.rglob("*.parquet*")), [])

    def test_retries_with_backoff_then_succeeds(self):
        attempts = [0]
        sleeps: list[float] = []

        def flaky(ds: Dataset, s: int | None) -> pl.DataFrame:
            attempts[0] += 1
            if attempts[0] == 1:
                raise RuntimeError("transient")
            return pl.DataFrame({"season": [s]})

        summary = execute_tasks(
            [_task("schedules", 2025)],
            self.paths,
            self.manifest,
            fetch=flaky,
            sleep=sleeps.append,
            log=_noop,
        )
        self.assertEqual(summary.written, [("schedules", 2025)])
        self.assertEqual(sleeps, [2.0])

    def test_unavailable_is_not_failure_and_unseasoned_uses_latest(self):
        def fetch_fn(ds: Dataset, s: int | None) -> pl.DataFrame:
            if s == 2027:
                raise DatasetNotAvailable("not yet posted")
            return pl.DataFrame({"player": ["A"]})

        summary = execute_tasks(
            [_task("schedules", 2027), _task("players", None)],
            self.paths,
            self.manifest,
            fetch=fetch_fn,
            sleep=_noop,
            log=_noop,
        )
        self.assertEqual(summary.unavailable, [("schedules", 2027)])
        self.assertEqual(summary.failed, [])
        self.assertTrue((self.paths.nflverse_dir / "players" / "latest.parquet").is_file())
        self.assertEqual(
            parquet_path(self.paths, Dataset("players", "load_players", seasonal=False), None),
            self.paths.nflverse_dir / "players" / "latest.parquet",
        )

    def test_nflreadpy_fetch_handles_404_and_empty(self):
        ds = Dataset(name="schedules", loader="fake_loader", seasonal=True)
        with patch("nfl_warehouse.download.nflreadpy", create=True) as mock_nfl:
            mock_nfl.fake_loader = MagicMock(return_value=pl.DataFrame())
            with self.assertRaises(DatasetNotAvailable):
                nflreadpy_fetch(ds, 2099)

            mock_nfl.fake_loader = MagicMock(side_effect=ConnectionError("404 Not Found"))
            with self.assertRaises(DatasetNotAvailable):
                nflreadpy_fetch(ds, 2099)

            mock_nfl.fake_loader = MagicMock(side_effect=RuntimeError("corrupt"))
            with self.assertRaises(RuntimeError):
                nflreadpy_fetch(ds, 2022)


if __name__ == "__main__":
    unittest.main()
