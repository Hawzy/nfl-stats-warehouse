"""Tests for manifest persistence and 30-day season finalization lifecycle."""

from datetime import date
from pathlib import Path
import sys
import tempfile
import unittest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.catalog import Dataset
from nfl_warehouse.lifecycle import DownloadTask, finalize_entries, plan_downloads
from nfl_warehouse.manifest import Manifest, ManifestCorruptError, ManifestEntry

DS = [
    Dataset(name="seasonal", loader="load_seasonal", seasonal=True, first_season=2020),
    Dataset(name="players", loader="load_players", seasonal=False, first_season=None),
]


def _keys(tasks: list[DownloadTask]) -> set[tuple[str, int | None, str]]:
    return {(t.dataset.name, t.season, t.reason) for t in tasks}


def _manifest_with(path: Path, *specs: tuple) -> Manifest:
    manifest = Manifest(path=path)
    for spec in specs:
        dataset, season, finalized = spec[0], spec[1], spec[2]
        dl_at = spec[3] if len(spec) > 3 else "2024-01-01T00:00:00+00:00"
        rel = f"{dataset}/{season}.parquet" if season is not None else f"{dataset}/latest.parquet"
        manifest.upsert(
            ManifestEntry(
                dataset=dataset,
                season=season,
                path=rel,
                rows=1,
                columns=1,
                bytes=100,
                downloaded_at=dl_at,
                nflreadpy_version="0.1.5",
                finalized=finalized,
            )
        )
    return manifest


class TestLifecycleAndManifest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.m_path = Path(self.tmpdir.name) / "_manifest.json"

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_plain_downloads_only_missing(self):
        m = _manifest_with(self.m_path, ("seasonal", 2020, False), ("seasonal", 2021, False))
        tasks = plan_downloads(DS, m, mode="plain", current_season=2022)
        self.assertEqual(
            _keys(tasks),
            {("seasonal", 2022, "missing"), ("players", None, "missing")},
        )

    def test_refresh_refetches_open_and_unseasoned_only(self):
        m = _manifest_with(
            self.m_path,
            ("seasonal", 2020, True),
            ("seasonal", 2021, False),
            ("seasonal", 2022, False),
            ("players", None, False),
        )
        tasks = plan_downloads(DS, m, mode="refresh", current_season=2022)
        self.assertEqual(
            _keys(tasks),
            {
                ("seasonal", 2021, "open"),
                ("seasonal", 2022, "open"),
                ("players", None, "unseasoned"),
            },
        )

    def test_force_respects_first_season_and_filter(self):
        m = Manifest.load(self.m_path)
        tasks = plan_downloads(DS, m, mode="force", current_season=2022, seasons=[2019, 2021])
        self.assertEqual(
            {(t.dataset.name, t.season) for t in tasks if t.dataset.seasonal},
            {("seasonal", 2021)},
        )

    def test_finalize_after_grace_window(self):
        m = _manifest_with(
            self.m_path,
            ("seasonal", 2021, False, "2022-03-15T00:00:00+00:00"),
            ("seasonal", 2022, False, "2023-02-20T00:00:00+00:00"),
        )
        count = finalize_entries(m, {2021: date(2022, 2, 13), 2022: date(2023, 2, 12)})
        self.assertEqual(count, 1)
        self.assertTrue(m.get("seasonal", 2021).finalized)
        self.assertFalse(m.get("seasonal", 2022).finalized)

    def test_manifest_round_trip(self):
        m = _manifest_with(self.m_path, ("players", None, False))
        m.save()
        loaded = Manifest.load(self.m_path)
        self.assertEqual(loaded.get("players", None).rows, 1)

    def test_corrupt_manifest_raises_actionable_error(self):
        self.m_path.write_text("{broken json", encoding="utf-8")
        with self.assertRaisesRegex(ManifestCorruptError, "--repair-manifest"):
            Manifest.load(self.m_path)

        self.m_path.write_text('{"version": 1, "entries": [{"dataset": "players"}]}', encoding="utf-8")
        with self.assertRaisesRegex(ManifestCorruptError, "--repair-manifest"):
            Manifest.load(self.m_path)


if __name__ == "__main__":
    unittest.main()
