"""Cross-platform warehouse footprint and runtime hardware telemetry."""

import ctypes
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import sys
from typing import Any

import duckdb
import polars as pl

from nfl_warehouse.manifest import Manifest
from nfl_warehouse.paths import WarehousePaths

__all__ = ["collect_warehouse_telemetry", "get_peak_rss_mb", "write_telemetry"]


class _ProcessMemoryCounters(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_uint32),
        ("PageFaultCount", ctypes.c_uint32),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def get_peak_rss_mb() -> float:
    """Return peak process resident set size (RSS / working set) in MiB."""
    if sys.platform == "win32":
        try:
            counters = _ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(_ProcessMemoryCounters)
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            kernel32.GetCurrentProcess.restype = ctypes.c_void_p
            psapi.GetProcessMemoryInfo.argtypes = [
                ctypes.c_void_p,
                ctypes.POINTER(_ProcessMemoryCounters),
                ctypes.c_uint32,
            ]
            psapi.GetProcessMemoryInfo.restype = ctypes.c_int
            handle = kernel32.GetCurrentProcess()
            ok = psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            )
            if ok:
                return round(counters.PeakWorkingSetSize / (1024 * 1024), 2)
        except Exception:
            return 0.0
    else:
        try:
            import resource

            maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # macOS reports bytes; Linux reports kilobytes.
            divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
            return round(maxrss / divisor, 2)
        except Exception:
            return 0.0
    return 0.0


def _pkg_version(name: str) -> str:
    """Return installed distribution version or 'unknown'."""
    try:
        return importlib.metadata.version(name)
    except Exception:
        return "unknown"


def _scan_parquet_footprint(paths: WarehousePaths) -> tuple[dict[str, dict[str, Any]], int, int]:
    """Summarize per-dataset Parquet disk usage using manifest + filesystem scan."""
    manifest = Manifest.load(paths.manifest_path)
    manifest_map: dict[tuple[str, int | None], dict[str, Any]] = {
        (e.dataset, e.season): {"rows": e.rows, "bytes": e.bytes}
        for e in manifest.entries()
    }

    datasets: dict[str, dict[str, Any]] = {}
    total_files = 0
    total_rows = 0

    if paths.nflverse_dir.is_dir():
        for ds_dir in sorted(p for p in paths.nflverse_dir.iterdir() if p.is_dir()):
            ds_name = ds_dir.name
            parquet_files = sorted(
                p for p in ds_dir.glob("*.parquet") if p.is_file() and p.suffix == ".parquet"
            )
            if not parquet_files:
                continue

            ds_bytes = 0
            ds_rows = 0
            seasons: list[int] = []
            for pf in parquet_files:
                file_bytes = pf.stat().st_size
                ds_bytes += file_bytes
                total_files += 1
                season: int | None = None
                if pf.stem.isdigit():
                    season = int(pf.stem)
                    seasons.append(season)
                entry = manifest_map.get((ds_name, season))
                if entry is not None:
                    ds_rows += int(entry["rows"])
            total_rows += ds_rows

            info: dict[str, Any] = {
                "files": len(parquet_files),
                "rows": ds_rows,
                "bytes": ds_bytes,
                "mb": round(ds_bytes / (1024 * 1024), 2),
            }
            if seasons:
                info["min_season"] = min(seasons)
                info["max_season"] = max(seasons)
            else:
                info["seasonal"] = False
            datasets[ds_name] = info

    sorted_datasets = dict(
        sorted(datasets.items(), key=lambda kv: kv[1]["bytes"], reverse=True)
    )
    return sorted_datasets, total_files, total_rows


def collect_warehouse_telemetry(
    paths: WarehousePaths,
    *,
    wall_seconds: float | None = None,
    peak_rss_mb: float | None = None,
    profile: str | None = None,
    audit_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build machine-readable warehouse footprint and hardware telemetry payload."""
    datasets, total_files, total_rows = _scan_parquet_footprint(paths)
    parquet_bytes = sum(info["bytes"] for info in datasets.values())
    db_bytes = paths.warehouse_path.stat().st_size if paths.warehouse_path.is_file() else 0
    total_bytes = parquet_bytes + db_bytes

    payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "os": sys.platform,
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "python_version": platform.python_version(),
            "duckdb_version": duckdb.__version__,
            "polars_version": pl.__version__,
            "nflreadpy_version": _pkg_version("nflreadpy"),
        },
        "profile": profile,
        "wall_seconds": round(wall_seconds, 3) if wall_seconds is not None else None,
        "peak_rss_mb": round(peak_rss_mb, 2) if peak_rss_mb is not None else get_peak_rss_mb(),
        "footprint": {
            "warehouse_db_bytes": db_bytes,
            "warehouse_db_mb": round(db_bytes / (1024 * 1024), 2),
            "parquet_total_bytes": parquet_bytes,
            "parquet_total_mb": round(parquet_bytes / (1024 * 1024), 2),
            "total_bytes": total_bytes,
            "total_mb": round(total_bytes / (1024 * 1024), 2),
            "dataset_count": len(datasets),
            "parquet_file_count": total_files,
            "total_rows": total_rows,
            "datasets": datasets,
        },
    }
    if audit_summary is not None:
        payload["audit"] = audit_summary
    return payload


def write_telemetry(path: Path | str, payload: dict[str, Any]) -> Path:
    """Atomically write telemetry JSON report."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.parent / f"{out_path.name}.tmp"
    text = json.dumps(payload, indent=2) + "\n"
    try:
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, out_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    return out_path
