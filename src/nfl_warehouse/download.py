"""Dataset download execution with atomic Parquet writes, retries, and manifest updates."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import gc
import os
from pathlib import Path
import time
from typing import Callable

import polars as pl

try:
    import nflreadpy
except ImportError:
    nflreadpy = None

from nfl_warehouse.catalog import Dataset
from nfl_warehouse.lifecycle import DownloadTask
from nfl_warehouse.manifest import Manifest, ManifestEntry
from nfl_warehouse.paths import WarehousePaths


class DatasetNotAvailable(Exception):
    """Raised when an nflverse dataset or season is not yet published."""


Fetcher = Callable[[Dataset, int | None], pl.DataFrame]


@dataclass
class DownloadSummary:
    """Outcome counters and details across planned download tasks."""

    written: list[tuple[str, int | None]] = field(default_factory=list)
    unavailable: list[tuple[str, int | None]] = field(default_factory=list)
    failed: list[tuple[str, int | None, str]] = field(default_factory=list)


def parquet_path(paths: WarehousePaths, ds: Dataset, season: int | None) -> Path:
    """Return destination Parquet path for a dataset and optional season."""
    dataset_name = ds.name if hasattr(ds, "name") else str(ds)
    filename = "latest.parquet" if season is None else f"{season}.parquet"
    return paths.nflverse_dir / dataset_name / filename


def _disable_nflreadpy_cache() -> None:
    """Disable nflreadpy's default CacheMode.MEMORY to avoid multi-GB RSS leaks."""
    if nflreadpy is None:
        return
    try:
        cfg_mod = getattr(nflreadpy, "config", None)
        if cfg_mod is not None and hasattr(cfg_mod, "update_config"):
            cfg_mod.update_config(cache_mode="off")
        if hasattr(nflreadpy, "clear_cache"):
            nflreadpy.clear_cache()
    except Exception:
        pass


def nflreadpy_fetch(ds: Dataset, season: int | None) -> pl.DataFrame:
    """Fetch dataset from nflreadpy, turning 404/empty responses into DatasetNotAvailable."""
    if nflreadpy is None:
        raise RuntimeError("nflreadpy is not installed")

    _disable_nflreadpy_cache()
    loader_fn = getattr(nflreadpy, ds.loader, None)
    if loader_fn is None:
        raise AttributeError(f"nflreadpy does not have loader {ds.loader!r}")

    kwargs = dict(ds.loader_kwargs)
    try:
        if ds.seasonal:
            df = loader_fn(seasons=season, **kwargs)
        else:
            df = loader_fn(**kwargs)
    except DatasetNotAvailable:
        raise
    except Exception as exc:
        msg = str(exc).lower()
        if (
            "404" in msg
            or "not found" in msg
            or "season must be between" in msg
            or ("season" in msg and "between" in msg)
        ):
            raise DatasetNotAvailable(str(exc)) from exc
        raise

    if df is None or len(df) == 0:
        raise DatasetNotAvailable(
            f"Empty dataframe returned for {ds.name} season={season}"
        )
    return df


def _utc_now() -> datetime:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


def _cleanup_tmp(tmp_path: Path) -> None:
    """Remove partial temporary file if present."""
    if tmp_path.exists():
        try:
            tmp_path.unlink()
        except OSError:
            pass


def execute_tasks(
    tasks: list[DownloadTask],
    paths: WarehousePaths,
    manifest: Manifest,
    *,
    fetch: Fetcher = nflreadpy_fetch,
    max_retries: int = 3,
    backoff_base: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
    now: Callable[[], datetime] = _utc_now,
) -> DownloadSummary:
    """Execute download tasks atomically and persist manifest progress."""
    _disable_nflreadpy_cache()
    summary = DownloadSummary()
    nflreadpy_version = (
        getattr(nflreadpy, "__version__", "unknown")
        if nflreadpy is not None
        else "unknown"
    )

    for task in tasks:
        final_path = parquet_path(paths, task.dataset, task.season)
        tmp_path = final_path.parent / f"{final_path.name}.tmp"

        for attempt in range(1, max_retries + 1):
            try:
                df = fetch(task.dataset, task.season)
                final_path.parent.mkdir(parents=True, exist_ok=True)
                df.write_parquet(tmp_path)
                os.replace(tmp_path, final_path)

                rows = getattr(df, "height", None)
                if rows is None:
                    rows = len(df)
                cols = getattr(df, "width", None)
                if cols is None:
                    cols = len(getattr(df, "columns", []))
                del df

                rel_path = final_path.relative_to(paths.nflverse_dir).as_posix()
                entry = ManifestEntry(
                    dataset=task.dataset.name,
                    season=task.season,
                    path=rel_path,
                    rows=int(rows),
                    columns=int(cols),
                    bytes=final_path.stat().st_size,
                    downloaded_at=now().isoformat(),
                    nflreadpy_version=nflreadpy_version,
                    finalized=False,
                )
                manifest.upsert(entry)
                manifest.save()
                if task.dataset.heavy:
                    gc.collect()

                summary.written.append((task.dataset.name, task.season))
                season_label = task.season if task.season is not None else "latest"
                log(f"{task.dataset.name} {season_label}: {rows:,} rows ({task.reason})")
                break
            except DatasetNotAvailable:
                _cleanup_tmp(tmp_path)
                summary.unavailable.append((task.dataset.name, task.season))
                log(f"{task.dataset.name} {task.season}: not yet available")
                break
            except Exception as exc:
                _cleanup_tmp(tmp_path)
                if attempt < max_retries:
                    log(
                        f"{task.dataset.name} {task.season}: attempt {attempt} "
                        f"failed ({exc}); retrying..."
                    )
                    sleep(backoff_base ** attempt)
                else:
                    summary.failed.append((task.dataset.name, task.season, str(exc)))

    return summary
