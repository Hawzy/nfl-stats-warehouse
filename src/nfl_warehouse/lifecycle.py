"""Season lifecycle and download planning for nflverse hydration."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from nfl_warehouse.catalog import Dataset
from nfl_warehouse.manifest import Manifest

GRACE_DAYS = 30
VALID_REASONS = {"missing", "open", "forced", "unseasoned"}


@dataclass(frozen=True)
class DownloadTask:
    """Planned fetch task for a single dataset and optional season."""

    dataset: Dataset
    season: int | None
    reason: str

    def __post_init__(self) -> None:
        if self.reason not in VALID_REASONS:
            raise ValueError(
                f"Invalid reason {self.reason!r}; must be one of {sorted(VALID_REASONS)}"
            )


def candidate_seasons(
    dataset: Dataset,
    *,
    current_season: int,
    seasons: list[int] | None = None,
) -> list[int]:
    """Return eligible seasons for a seasonal dataset respecting first_season."""
    if not dataset.seasonal:
        return []
    if seasons is not None:
        return [
            s
            for s in seasons
            if dataset.first_season is None or s >= dataset.first_season
        ]
    first = dataset.first_season if dataset.first_season is not None else current_season
    return list(range(first, current_season + 1))


def plan_downloads(
    datasets: list[Dataset],
    manifest: Manifest,
    *,
    mode: str,
    current_season: int,
    seasons: list[int] | None = None,
) -> list[DownloadTask]:
    """Plan download tasks according to mode ('plain', 'refresh', 'force') and manifest."""
    if mode not in {"plain", "refresh", "force"}:
        raise ValueError(
            f"Invalid mode {mode!r}; must be one of 'plain', 'refresh', 'force'"
        )

    tasks: list[DownloadTask] = []

    for ds in datasets:
        if ds.seasonal:
            for season in candidate_seasons(
                ds, current_season=current_season, seasons=seasons
            ):
                entry = manifest.get(ds.name, season)
                if mode == "force":
                    tasks.append(DownloadTask(dataset=ds, season=season, reason="forced"))
                elif mode == "plain":
                    if entry is None:
                        tasks.append(
                            DownloadTask(dataset=ds, season=season, reason="missing")
                        )
                elif mode == "refresh":
                    if entry is None:
                        tasks.append(
                            DownloadTask(dataset=ds, season=season, reason="missing")
                        )
                    elif not entry.finalized:
                        tasks.append(
                            DownloadTask(dataset=ds, season=season, reason="open")
                        )
        else:
            entry = manifest.get(ds.name, None)
            if mode == "force":
                tasks.append(DownloadTask(dataset=ds, season=None, reason="forced"))
            elif mode == "plain":
                if entry is None:
                    tasks.append(DownloadTask(dataset=ds, season=None, reason="missing"))
            elif mode == "refresh":
                reason = "missing" if entry is None else "unseasoned"
                tasks.append(DownloadTask(dataset=ds, season=None, reason=reason))

    return tasks


def finalize_entries(
    manifest: Manifest,
    season_end_dates: dict[int, date],
    grace_days: int = GRACE_DAYS,
) -> int:
    """Finalize open seasonal entries downloaded >= grace_days after season end."""
    newly_finalized = 0
    cutoff_delta = timedelta(days=grace_days)

    for entry in manifest.entries():
        if entry.finalized or entry.season is None:
            continue
        if entry.season not in season_end_dates:
            continue

        end_date = season_end_dates[entry.season]
        dl_date = datetime.fromisoformat(entry.downloaded_at).date()
        if dl_date >= end_date + cutoff_delta:
            entry.finalized = True
            newly_finalized += 1

    return newly_finalized
