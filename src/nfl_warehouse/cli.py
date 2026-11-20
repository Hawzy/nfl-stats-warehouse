"""CLI entry point for hydrating and rebuilding the NFL stats warehouse."""

import argparse
from datetime import date, datetime
from pathlib import Path
import sys
import time
from typing import Callable

import duckdb

from nfl_warehouse.catalog import VALID_PROFILES, get_datasets
from nfl_warehouse.connect import connect
from nfl_warehouse.download import (
    DownloadSummary,
    Fetcher,
    execute_tasks,
    nflreadpy_fetch,
)
from nfl_warehouse.lifecycle import candidate_seasons, finalize_entries, plan_downloads
from nfl_warehouse.manifest import Manifest, ManifestEntry
from nfl_warehouse.mart import build_mart_views
from nfl_warehouse.paths import WarehousePaths, default_scoring_config_path
from nfl_warehouse.player_dim import build_player_dim
from nfl_warehouse.scoring import (
    build_player_week_points,
    build_site_scoring,
    build_team_defense_week_points,
    load_scoring_config,
)
from nfl_warehouse.telemetry import collect_warehouse_telemetry, write_telemetry
from nfl_warehouse.views import create_views

__all__ = ["main", "parse_args", "run"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for nfl-warehouse."""
    parser = argparse.ArgumentParser(
        prog="nfl-warehouse",
        description="Hydrate and rebuild the public DuckDB + nflverse NFL stats warehouse.",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download open and unseasoned datasets, then rebuild views/tables",
    )
    mode_group.add_argument(
        "--force",
        action="store_true",
        help="Force re-download of specified --datasets and/or --seasons",
    )
    mode_group.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild DuckDB views, tables, and mart views from existing Parquet only",
    )
    mode_group.add_argument(
        "--status",
        action="store_true",
        help="Print dataset manifest summary without opening the warehouse",
    )

    parser.add_argument(
        "--profile",
        choices=VALID_PROFILES,
        default="core",
        help="Hydration profile: lite (excludes pbp), core (default), or full (all optional)",
    )
    parser.add_argument(
        "--no-pbp",
        action="store_true",
        help="Exclude heavy play-by-play (pbp) dataset",
    )
    parser.add_argument(
        "--include-optional",
        action="store_true",
        help="Include optional datasets (equivalent to --profile full)",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        type=str,
        help="Explicit dataset names to hydrate",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        help="Specific seasons to hydrate",
    )
    parser.add_argument(
        "--scoring-config",
        type=str,
        default=None,
        help="Path to custom scoring JSON config (defaults to config/scoring.json)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Override warehouse data directory (overrides NFL_WAREHOUSE_DIR)",
    )
    parser.add_argument(
        "--current-season",
        type=int,
        default=None,
        help="Override current NFL season year",
    )
    parser.add_argument(
        "--telemetry-out",
        type=str,
        default=None,
        help="Write JSON footprint & hardware telemetry report to this path",
    )

    return parser.parse_args(argv)


def _determine_current_season(override: int | None) -> int:
    """Resolve current NFL season from override or nflreadpy."""
    if override is not None:
        return override
    try:
        import nflreadpy

        if hasattr(nflreadpy, "get_current_season"):
            season = nflreadpy.get_current_season()
            if season is not None:
                return int(season)
    except Exception:
        pass
    return datetime.now().year


def _report_status(paths: WarehousePaths, log: Callable[[str], None]) -> int:
    """Print dataset summary from manifest without opening DuckDB."""
    manifest = Manifest.load(paths.manifest_path)
    entries = manifest.entries()
    if not entries:
        log("manifest is empty: no datasets recorded")
        return 0

    by_ds: dict[str, list[ManifestEntry]] = {}
    for entry in entries:
        by_ds.setdefault(entry.dataset, []).append(entry)

    for ds_name in sorted(by_ds.keys()):
        ds_entries = by_ds[ds_name]
        seasons = [e.season for e in ds_entries if e.season is not None]
        if seasons:
            min_s, max_s = min(seasons), max(seasons)
            season_str = f"{min_s}-{max_s}" if min_s != max_s else str(min_s)
        else:
            season_str = "unseasoned"

        total_rows = sum(e.rows for e in ds_entries)
        total_mb = sum(e.bytes for e in ds_entries) / (1024 * 1024)
        latest_dl = max(e.downloaded_at for e in ds_entries)
        open_seasons = [e.season for e in ds_entries if not e.finalized]

        log(
            f"{ds_name}: seasons {season_str}, {len(ds_entries)} files, "
            f"{total_rows:,} rows, {total_mb:.2f} MB, latest {latest_dl}, open: {open_seasons}"
        )
    return 0


def _compute_season_end_dates(paths: WarehousePaths) -> dict[int, date]:
    """Derive season end dates by scanning schedules Parquet files."""
    schedules_dir = paths.nflverse_dir / "schedules"
    if not schedules_dir.is_dir():
        return {}

    parquet_files = [
        p
        for p in schedules_dir.glob("*.parquet")
        if p.is_file() and p.suffix == ".parquet"
    ]
    if not parquet_files:
        return {}

    escaped_glob = (schedules_dir.resolve().as_posix() + "/*.parquet").replace("'", "''")
    season_end_dates: dict[int, date] = {}
    with duckdb.connect(":memory:") as mem_con:
        rows = mem_con.execute(
            f"SELECT season, max(gameday)::DATE "
            f"FROM read_parquet('{escaped_glob}', union_by_name=true) "
            f"GROUP BY season"
        ).fetchall()
        for season_val, dt_val in rows:
            if season_val is not None and dt_val is not None:
                if isinstance(dt_val, datetime):
                    dt_val = dt_val.date()
                elif isinstance(dt_val, str):
                    dt_val = date.fromisoformat(dt_val)
                season_end_dates[int(season_val)] = dt_val
    return season_end_dates


def run(
    argv: list[str] | None = None,
    *,
    paths: WarehousePaths | None = None,
    fetch: Fetcher = nflreadpy_fetch,
    current_season: int | None = None,
    connect_fn: Callable[..., duckdb.DuckDBPyConnection] = connect,
    log: Callable[[str], None] = print,
) -> int:
    """Execute warehouse hydration, rebuild, or status check."""
    started = time.perf_counter()
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2

    if args.force and not args.datasets and not args.seasons:
        log("--force needs --datasets and/or --seasons (refusing to re-download all history)")
        return 2

    if paths is None:
        if args.data_dir is not None:
            paths = WarehousePaths.for_data_dir(args.data_dir)
        else:
            paths = WarehousePaths.default()

    eff_current_season = _determine_current_season(
        args.current_season if args.current_season is not None else current_season
    )
    eff_profile = "full" if args.include_optional else args.profile

    try:
        selected_datasets = get_datasets(
            include_optional=args.include_optional,
            names=args.datasets,
            profile=args.profile,
            exclude_pbp=args.no_pbp,
        )
    except ValueError as exc:
        log(f"Usage error: {exc}")
        return 2

    if args.status:
        return _report_status(paths, log)

    manifest = Manifest.load(paths.manifest_path)
    summary = DownloadSummary()
    skipped = 0

    if not args.rebuild:
        mode = "force" if args.force else ("refresh" if args.refresh else "plain")
        tasks = plan_downloads(
            selected_datasets,
            manifest,
            mode=mode,
            current_season=eff_current_season,
            seasons=args.seasons,
        )
        total_candidates = sum(
            len(
                candidate_seasons(
                    ds, current_season=eff_current_season, seasons=args.seasons
                )
            )
            if ds.seasonal
            else 1
            for ds in selected_datasets
        )
        skipped = max(0, total_candidates - len(tasks))

        schedules_tasks = [t for t in tasks if t.dataset.name == "schedules"]
        other_tasks = [t for t in tasks if t.dataset.name != "schedules"]
        summary = execute_tasks(
            schedules_tasks + other_tasks,
            paths,
            manifest,
            fetch=fetch,
            log=log,
        )

        season_end_dates = _compute_season_end_dates(paths)
        finalize_entries(manifest, season_end_dates)
        manifest.save()

    try:
        con = connect_fn(paths, log=log)
    except duckdb.IOException:
        log(
            f"warehouse is in use by another process; close it and retry: "
            f"{paths.warehouse_path}"
        )
        return 1

    try:
        view_datasets = [
            d
            for d in get_datasets(profile="full")
            if d in selected_datasets or (paths.nflverse_dir / d.name).exists()
        ]
        create_views(con, paths, view_datasets, log=log)

        scoring_path = (
            Path(args.scoring_config).expanduser().resolve()
            if args.scoring_config
            else default_scoring_config_path(paths)
        )
        if scoring_path.is_file():
            scoring_config = load_scoring_config(scoring_path)
            build_site_scoring(con, scoring_config)
            build_player_week_points(con, scoring_config, log=log)
            build_team_defense_week_points(con, scoring_config, log=log)
        else:
            log(f"Scoring config not found at {scoring_path}; skipping scoring tables")

        build_player_dim(con, log=log)
        build_mart_views(con, log=log)
    finally:
        con.close()

    wall_seconds = time.perf_counter() - started
    if args.telemetry_out:
        telemetry = collect_warehouse_telemetry(
            paths,
            wall_seconds=wall_seconds,
            profile="lite" if args.no_pbp and eff_profile == "core" else eff_profile,
        )
        write_telemetry(args.telemetry_out, telemetry)

    log(
        f"done: {len(summary.written)} written, {skipped} up to date, "
        f"{len(summary.unavailable)} not yet available, {len(summary.failed)} failed "
        f"({wall_seconds:.2f}s)"
    )
    return 1 if summary.failed else 0


def main(argv: list[str] | None = None) -> int:
    """Entrypoint wrapper ensuring UTF-8 output on Windows consoles."""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    return run(argv)
