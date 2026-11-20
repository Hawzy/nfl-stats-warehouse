"""JSON manifest tracking nflverse raw Parquet files and finalization state."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import polars as pl

__all__ = ["Manifest", "ManifestCorruptError", "ManifestEntry"]


class ManifestCorruptError(ValueError):
    """Raised when _manifest.json exists but cannot be parsed cleanly."""


@dataclass
class ManifestEntry:
    """Metadata record for a single downloaded dataset file."""

    dataset: str
    season: int | None
    path: str
    rows: int
    columns: int
    bytes: int
    downloaded_at: str
    nflreadpy_version: str
    finalized: bool = False


class Manifest:
    """In-memory manifest of downloaded files backed by an atomic JSON file."""

    def __init__(
        self,
        path: Path,
        entries: dict[tuple[str, int | None], ManifestEntry] | None = None,
    ) -> None:
        self.path = Path(path)
        self._entries: dict[tuple[str, int | None], ManifestEntry] = (
            dict(entries) if entries is not None else {}
        )

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        """Load manifest from path, or return an empty manifest if missing."""
        path = Path(path)
        if not path.is_file():
            return cls(path=path)

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ManifestCorruptError(
                f"Corrupt manifest at {path}: {exc}. "
                "Run 'python -m nfl_warehouse --repair-manifest' to rebuild it from existing Parquet files."
            ) from exc

        if not isinstance(data, dict) or not isinstance(data.get("entries", []), list):
            raise ManifestCorruptError(
                f"Corrupt manifest at {path}: expected JSON object with 'entries' list. "
                "Run 'python -m nfl_warehouse --repair-manifest' to rebuild it from existing Parquet files."
            )

        entries: dict[tuple[str, int | None], ManifestEntry] = {}
        try:
            for item in data.get("entries", []):
                entry = ManifestEntry(**item)
                entries[(entry.dataset, entry.season)] = entry
        except (TypeError, KeyError, AttributeError) as exc:
            raise ManifestCorruptError(
                f"Corrupt manifest entry in {path}: {exc}. "
                "Run 'python -m nfl_warehouse --repair-manifest' to rebuild it from existing Parquet files."
            ) from exc
        return cls(path=path, entries=entries)

    @classmethod
    def rebuild_from_disk(
        cls,
        nflverse_dir: Path,
        manifest_path: Path,
        *,
        nflreadpy_version: str = "unknown",
    ) -> "Manifest":
        """Reconstruct manifest entries by scanning existing Parquet files on disk."""
        nflverse_dir = Path(nflverse_dir)
        entries: dict[tuple[str, int | None], ManifestEntry] = {}
        if nflverse_dir.is_dir():
            for ds_dir in sorted(p for p in nflverse_dir.iterdir() if p.is_dir()):
                ds_name = ds_dir.name
                parquet_files = sorted(
                    p
                    for p in ds_dir.glob("*.parquet")
                    if p.is_file() and p.suffix == ".parquet"
                )
                for pf in parquet_files:
                    season = int(pf.stem) if pf.stem.isdigit() else None
                    try:
                        schema = pl.read_parquet_schema(pf)
                        rows = int(pl.scan_parquet(pf).select(pl.len()).collect().item())
                    except Exception as exc:
                        raise ValueError(f"Unreadable Parquet file {pf}: {exc}") from exc
                    stat = pf.stat()
                    dl_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
                    entry = ManifestEntry(
                        dataset=ds_name,
                        season=season,
                        path=pf.relative_to(nflverse_dir).as_posix(),
                        rows=rows,
                        columns=len(schema),
                        bytes=stat.st_size,
                        downloaded_at=dl_at,
                        nflreadpy_version=nflreadpy_version,
                        finalized=False,
                    )
                    entries[(ds_name, season)] = entry
        return cls(path=manifest_path, entries=entries)

    def save(self) -> None:
        """Atomically write the manifest using a temporary file and os.replace."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.parent / f"{self.path.name}.tmp"
        payload = {
            "version": 1,
            "entries": [asdict(e) for e in self.entries()],
        }
        text = json.dumps(payload, indent=2)
        try:
            tmp_path.write_text(text, encoding="utf-8")
            os.replace(tmp_path, self.path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def get(self, dataset: str, season: int | None) -> ManifestEntry | None:
        """Return the ManifestEntry for (dataset, season), or None if absent."""
        return self._entries.get((dataset, season))

    def upsert(self, entry: ManifestEntry) -> None:
        """Insert or update a ManifestEntry."""
        self._entries[(entry.dataset, entry.season)] = entry

    def entries(self) -> list[ManifestEntry]:
        """Return all manifest entries."""
        return list(self._entries.values())
