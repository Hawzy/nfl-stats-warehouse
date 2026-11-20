"""JSON manifest tracking nflverse raw Parquet files and finalization state."""

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path


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

        data = json.loads(path.read_text(encoding="utf-8"))
        entries: dict[tuple[str, int | None], ManifestEntry] = {}
        for item in data.get("entries", []):
            entry = ManifestEntry(**item)
            entries[(entry.dataset, entry.season)] = entry
        return cls(path=path, entries=entries)

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
