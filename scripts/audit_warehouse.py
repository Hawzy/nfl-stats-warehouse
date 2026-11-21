"""Automated live data-integrity audit and hardware telemetry emitter."""

import argparse
import json
from pathlib import Path
import re
import sys
import time
from typing import Any

import duckdb

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nfl_warehouse.catalog import VALID_PROFILES, get_datasets
from nfl_warehouse.connect import connect
from nfl_warehouse.mart import schedule_kickoff_sql
from nfl_warehouse.paths import WarehousePaths
from nfl_warehouse.telemetry import (
    collect_warehouse_telemetry,
    get_peak_rss_mb,
    write_telemetry,
)

__all__ = ["run_audit", "main"]

_ABS_PATH_RE = re.compile(
    r"([A-Za-z]:[/\\]|\\\\|/Users/|/home/|/tmp/|/workspace/|/var/|/opt/)"
)

_NON_CANONICAL_TEAMS: tuple[str, ...] = (
    "LAR", "JAC", "STL", "SD", "OAK", "WSH", "SL", "ARZ", "BLT", "CLV", "HST"
)

_FULL_HISTORY_FLOORS: dict[str, int] = {
    "pbp": 1_000_000,
    "player_stats_week": 300_000,
    "team_stats_week": 10_000,
    "schedules": 5_000,
    "rosters_weekly": 500_000,
    "snap_counts": 200_000,
    "injuries": 50_000,
    "depth_charts": 1_000_000,
    "nextgen_passing": 4_000,
    "nextgen_rushing": 4_000,
    "nextgen_receiving": 10_000,
    "ff_opportunity": 80_000,
    "players": 10_000,
    "ff_playerids": 5_000,
    "teams": 32,
    "trades": 1_000,
    "participation": 100_000,
    "ftn_charting": 100_000,
    "pfr_adv_pass": 1_000,
    "pfr_adv_rush": 3_000,
    "pfr_adv_rec": 5_000,
    "pfr_adv_def": 5_000,
    "draft_picks": 5_000,
    "combine": 5_000,
    "contracts": 10_000,
}


def _check_view_portability(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Verify every nfl.* view uses relative Parquet paths with zero hardcoded OS paths."""
    rows = con.execute(
        "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = 'nfl' ORDER BY view_name"
    ).fetchall()
    bad_views: dict[str, str] = {}
    for view_name, sql in rows:
        if _ABS_PATH_RE.search(sql or ""):
            bad_views[view_name] = sql
    passed = len(rows) > 0 and not bad_views
    return {
        "name": "view_portability",
        "status": "PASS" if passed else "FAIL",
        "view_count": len(rows),
        "non_portable_views": bad_views,
    }


def _check_row_count_floors(
    con: duckdb.DuckDBPyConnection,
    *,
    profile: str,
    enforce_full_floors: bool,
) -> dict[str, Any]:
    """Verify expected nfl.* views exist and meet non-empty / historical row floors."""
    existing = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'nfl'"
        ).fetchall()
    }
    expected_datasets = get_datasets(profile=profile)
    missing: list[str] = []
    below_floor: list[dict[str, Any]] = []
    counts: dict[str, int] = {}

    for ds in expected_datasets:
        if ds.name not in existing:
            missing.append(ds.name)
            continue
        cnt = con.execute(f"SELECT count(*) FROM nfl.{ds.name}").fetchone()[0]
        counts[ds.name] = int(cnt)
        if cnt <= 0:
            below_floor.append({"dataset": ds.name, "rows": int(cnt), "floor": 1})
            continue
        if enforce_full_floors and ds.name in _FULL_HISTORY_FLOORS:
            floor = _FULL_HISTORY_FLOORS[ds.name]
            if cnt < floor:
                below_floor.append({"dataset": ds.name, "rows": int(cnt), "floor": floor})

    passed = not missing and not below_floor
    return {
        "name": "row_count_floors",
        "status": "PASS" if passed else "FAIL",
        "profile": profile,
        "missing_views": missing,
        "below_floor": below_floor,
        "row_counts": counts,
    }


def _check_player_dim_integrity(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Verify player_dim PK uniqueness and summarize cross-platform ID coverage."""
    total, distinct_gsis, null_gsis = con.execute(
        """
        SELECT
            count(*),
            count(DISTINCT gsis_id),
            count(*) FILTER (WHERE gsis_id IS NULL OR trim(gsis_id) = '')
        FROM main.player_dim
        """
    ).fetchone()

    id_cols = (
        "mfl_id",
        "yahoo_id",
        "sleeper_id",
        "espn_id",
        "fantasypros_id",
        "pfr_id",
        "sportradar_id",
        "rotowire_id",
        "pff_id",
    )
    overall_cov: dict[str, dict[str, Any]] = {}
    for col in id_cols:
        non_null = con.execute(
            f"SELECT count(*) FROM main.player_dim WHERE {col} IS NOT NULL"
        ).fetchone()[0]
        overall_cov[col] = {
            "count": int(non_null),
            "pct": round((100.0 * non_null / total), 2) if total else 0.0,
        }

    active_skill_cov: dict[str, Any] = {}
    latest_season = con.execute(
        "SELECT max(CAST(season AS INT)) FROM nfl.player_stats_week"
    ).fetchone()[0]
    if latest_season is not None:
        agg_parts = ", ".join(f"count(pd.{col}) AS has_{col}" for col in id_cols)
        row = con.execute(
            f"""
            WITH active AS (
                SELECT DISTINCT CAST(player_id AS VARCHAR) AS gsis_id
                FROM nfl.player_stats_week
                WHERE CAST(season AS INT) = ?
                  AND position IN ('QB', 'RB', 'WR', 'TE')
            )
            SELECT count(*), {agg_parts}
            FROM active a
            LEFT JOIN main.player_dim pd ON pd.gsis_id = a.gsis_id
            """,
            [int(latest_season)],
        ).fetchone()
        active_total = int(row[0])
        active_skill_cov["season"] = int(latest_season)
        active_skill_cov["total_skill_players"] = active_total
        active_skill_cov["ids"] = {
            col: {
                "count": int(row[idx + 1]),
                "missing": active_total - int(row[idx + 1]),
                "pct": round((100.0 * int(row[idx + 1]) / active_total), 2)
                if active_total
                else 0.0,
            }
            for idx, col in enumerate(id_cols)
        }

        yahoo_by_draft = con.execute(
            """
            WITH active AS (
                SELECT DISTINCT CAST(player_id AS VARCHAR) AS gsis_id
                FROM nfl.player_stats_week
                WHERE CAST(season AS INT) = ?
                  AND position IN ('QB', 'RB', 'WR', 'TE')
            )
            SELECT pd.draft_year, count(*) AS total, count(pd.yahoo_id) AS has_yahoo
            FROM active a
            JOIN main.player_dim pd ON pd.gsis_id = a.gsis_id
            WHERE pd.yahoo_id IS NULL
            GROUP BY 1
            ORDER BY 1 DESC NULLS LAST
            """,
            [int(latest_season)],
        ).fetchall()
        active_skill_cov["missing_yahoo_by_draft_year"] = [
            {"draft_year": yr, "missing": int(cnt)} for yr, cnt, _ in yahoo_by_draft
        ]

    secondary_collisions: dict[str, int] = {}
    for col in id_cols:
        dup_cnt = con.execute(
            f"SELECT count(*) FROM ("
            f"SELECT {col} FROM main.player_dim WHERE {col} IS NOT NULL GROUP BY 1 HAVING count(*) > 1"
            f")"
        ).fetchone()[0]
        if dup_cnt > 0:
            secondary_collisions[col] = int(dup_cnt)

    passed = total > 0 and total == distinct_gsis and null_gsis == 0
    return {
        "name": "player_dim_integrity",
        "status": "PASS" if passed else "FAIL",
        "total_players": int(total),
        "distinct_gsis": int(distinct_gsis),
        "null_or_blank_gsis": int(null_gsis),
        "secondary_id_collisions": secondary_collisions,
        "overall_coverage": overall_cov,
        "active_skill_coverage": active_skill_cov,
    }


def _check_team_abbreviations(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Verify mart.* views and team_defense_week_points expose canonical team codes only."""
    bad_placeholders = ", ".join(f"'{t}'" for t in _NON_CANONICAL_TEAMS)
    targets = [
        ("mart.nextgen_passing_weekly", "team"),
        ("mart.nextgen_rushing_weekly", "team"),
        ("mart.nextgen_receiving_weekly", "team"),
        ("mart.player_team_week", "team"),
        ("mart.depth_charts_weekly", "team"),
        ("mart.player_depth_rank_week", "team"),
        ("main.team_defense_week_points", "team"),
    ]
    violations: dict[str, list[str]] = {}
    checked: list[str] = []

    for tbl, col in targets:
        try:
            rows = con.execute(
                f"SELECT DISTINCT {col} FROM {tbl} "
                f"WHERE season >= 2020 AND {col} IN ({bad_placeholders}) ORDER BY 1"
            ).fetchall()
            checked.append(tbl)
            if rows:
                violations[tbl] = [r[0] for r in rows]
        except duckdb.Error:
            continue

    passed = len(checked) > 0 and not violations
    return {
        "name": "team_abbreviation_consistency",
        "status": "PASS" if passed else "FAIL",
        "checked_relations": checked,
        "violations": violations,
    }


def _check_scoring_reconciliation(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Reconcile main.player_week_points ('ppr') 0-diff vs nflverse fantasy_points_ppr."""
    matched_rows, diff_count, max_abs_diff = con.execute(
        """
        SELECT
            count(*),
            count(*) FILTER (WHERE abs(p.points - psw.fantasy_points_ppr) > 1e-9),
            coalesce(max(abs(p.points - psw.fantasy_points_ppr)), 0.0)
        FROM main.player_week_points p
        JOIN nfl.player_stats_week psw
          ON psw.player_id = p.player_id
         AND CAST(psw.season AS INT) = p.season
         AND CAST(psw.week AS INT) = p.week
         AND CAST(psw.season_type AS VARCHAR) = p.season_type
        WHERE p.site = 'ppr'
          AND (p.position IS NULL OR p.position != 'K')
          AND psw.fantasy_points_ppr IS NOT NULL
        """
    ).fetchone()
    unscored_non_k_ppr = con.execute(
        """
        SELECT count(*)
        FROM nfl.player_stats_week psw
        LEFT JOIN main.player_week_points p
          ON p.player_id = psw.player_id
         AND p.season = CAST(psw.season AS INT)
         AND p.week = CAST(psw.week AS INT)
         AND p.season_type = CAST(psw.season_type AS VARCHAR)
         AND p.site = 'ppr'
        WHERE psw.player_id IS NOT NULL
          AND trim(CAST(psw.player_id AS VARCHAR)) != ''
          AND (psw.position IS NULL OR psw.position != 'K')
          AND coalesce(psw.fantasy_points_ppr, 0.0) != 0.0
          AND p.player_id IS NULL
        """
    ).fetchone()[0]

    site_counts = dict(
        con.execute(
            "SELECT site, count(*) FROM main.player_week_points GROUP BY site ORDER BY site"
        ).fetchall()
    )
    kicker_rows = con.execute(
        "SELECT count(*) FROM main.player_week_points WHERE position = 'K'"
    ).fetchone()[0]
    dst_rows = 0
    try:
        dst_rows = con.execute("SELECT count(*) FROM main.team_defense_week_points").fetchone()[0]
    except duckdb.Error:
        dst_rows = 0

    has_dst_source = False
    try:
        con.execute("SELECT 1 FROM nfl.team_stats_week LIMIT 1").fetchone()
        has_dst_source = True
    except duckdb.Error:
        has_dst_source = False

    expected_sites = {"ppr", "half_ppr", "standard", "yahoo"}
    missing_sites = sorted(expected_sites - set(site_counts.keys()))
    passed = (
        int(matched_rows) > 0
        and int(diff_count) == 0
        and float(max_abs_diff) <= 1e-9
        and int(unscored_non_k_ppr) == 0
        and not missing_sites
        and int(kicker_rows) > 0
        and (not has_dst_source or int(dst_rows) > 0)
    )
    return {
        "name": "scoring_reconciliation",
        "status": "PASS" if passed else "FAIL",
        "ppr_matched_rows": int(matched_rows),
        "ppr_diff_count": int(diff_count),
        "ppr_max_abs_diff": float(max_abs_diff),
        "unscored_non_k_ppr_rows": int(unscored_non_k_ppr),
        "missing_sites": missing_sites,
        "site_row_counts": {str(k): int(v) for k, v in site_counts.items()},
        "kicker_rows": int(kicker_rows),
        "dst_rows": int(dst_rows),
    }


def _check_point_in_time_guardrails(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Verify mart.* lookahead guardrails across NextGen, depth charts, rosters, and participation."""
    issues: list[str] = []

    # 1. NextGen week=0 rollups excluded
    ng_counts: dict[str, int] = {}
    for view_name in ("nextgen_passing_weekly", "nextgen_rushing_weekly", "nextgen_receiving_weekly"):
        try:
            bad = con.execute(
                f"SELECT count(*) FROM mart.{view_name} WHERE week IS NULL OR week < 1"
            ).fetchone()[0]
            ng_counts[view_name] = int(bad)
            if bad > 0:
                issues.append(f"mart.{view_name} has {bad} week<1 rows")
        except duckdb.Error:
            issues.append(f"mart.{view_name} missing")

    # 2. No mart.* view references participation
    mart_views = con.execute(
        "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = 'mart'"
    ).fetchall()
    part_refs = [vn for vn, sql in mart_views if "participation" in (sql or "").lower()]
    if part_refs:
        issues.append(f"mart views reference participation: {part_refs}")

    # 3. Depth charts 2025+ coverage and zero post-game snapshot leakage
    dc_recent_rows = 0
    dc_lookahead_rows = 0
    try:
        has_dt_raw = con.execute(
            "SELECT count(*) FROM nfl.depth_charts WHERE dt IS NOT NULL"
        ).fetchone()[0]
        dc_recent_rows = con.execute(
            "SELECT count(*) FROM mart.depth_charts_weekly WHERE source_format = 'dt_pregame'"
        ).fetchone()[0]
        if has_dt_raw > 0 and dc_recent_rows == 0:
            issues.append("mart.depth_charts_weekly has 0 dt_pregame rows despite raw dt data")
        sched_cols = {
            r[0].lower() for r in con.execute("DESCRIBE nfl.schedules").fetchall()
        }
        kickoff_expr = schedule_kickoff_sql(sched_cols, alias="s")
        dc_lookahead_rows = con.execute(
            f"""
            WITH sched_games AS (
                SELECT s.season, s.week, COALESCE(m.team, s.home_team) AS team, {kickoff_expr} AS kickoff_ts
                FROM nfl.schedules s
                LEFT JOIN mart.team_abbr_map m ON upper(trim(s.home_team)) = m.raw_abbr
                UNION ALL
                SELECT s.season, s.week, COALESCE(m.team, s.away_team) AS team, {kickoff_expr} AS kickoff_ts
                FROM nfl.schedules s
                LEFT JOIN mart.team_abbr_map m ON upper(trim(s.away_team)) = m.raw_abbr
            )
            SELECT count(*)
            FROM mart.depth_charts_weekly dc
            JOIN sched_games sg
              ON sg.season = dc.season AND sg.week = dc.week AND sg.team = dc.team
            WHERE dc.snapshot_dt IS NOT NULL
              AND sg.kickoff_ts IS NOT NULL
              AND dc.snapshot_dt >= sg.kickoff_ts
            """
        ).fetchone()[0]
        if dc_lookahead_rows > 0:
            issues.append(f"mart.depth_charts_weekly has {dc_lookahead_rows} post-game snapshot rows")
    except duckdb.Error as exc:
        issues.append(f"depth_charts check error: {exc}")

    # 4. player_team_week key uniqueness
    ptw_dupes = 0
    try:
        ptw_dupes = con.execute(
            """
            SELECT count(*) FROM (
                SELECT season, week, gsis_id
                FROM mart.player_team_week
                GROUP BY 1, 2, 3
                HAVING count(*) > 1
            )
            """
        ).fetchone()[0]
        if ptw_dupes > 0:
            issues.append(f"mart.player_team_week has {ptw_dupes} duplicate keys")
    except duckdb.Error as exc:
        issues.append(f"player_team_week missing: {exc}")

    # 5. player_depth_rank_week key uniqueness
    pdr_dupes = 0
    try:
        pdr_dupes = con.execute(
            """
            SELECT count(*) FROM (
                SELECT season, week, team, gsis_id
                FROM mart.player_depth_rank_week
                GROUP BY 1, 2, 3, 4
                HAVING count(*) > 1
            )
            """
        ).fetchone()[0]
        if pdr_dupes > 0:
            issues.append(f"mart.player_depth_rank_week has {pdr_dupes} duplicate keys")
    except duckdb.Error as exc:
        issues.append(f"player_depth_rank_week missing: {exc}")

    return {
        "name": "point_in_time_guardrails",
        "status": "PASS" if not issues else "FAIL",
        "nextgen_invalid_week_counts": ng_counts,
        "participation_references": part_refs,
        "depth_charts_dt_pregame_rows": int(dc_recent_rows),
        "depth_charts_lookahead_rows": int(dc_lookahead_rows),
        "player_team_week_duplicates": int(ptw_dupes),
        "player_depth_rank_week_duplicates": int(pdr_dupes),
        "issues": issues,
    }


def run_audit(
    paths: WarehousePaths,
    *,
    profile: str = "core",
    enforce_full_floors: bool = True,
) -> dict[str, Any]:
    """Run all warehouse integrity checks in strict read-only mode."""
    started = time.perf_counter()
    with connect(paths, read_only=True) as con:
        checks = [
            _check_view_portability(con),
            _check_row_count_floors(
                con, profile=profile, enforce_full_floors=enforce_full_floors
            ),
            _check_player_dim_integrity(con),
            _check_team_abbreviations(con),
            _check_scoring_reconciliation(con),
            _check_point_in_time_guardrails(con),
        ]

    all_passed = all(c["status"] == "PASS" for c in checks)
    return {
        "overall_status": "PASS" if all_passed else "FAIL",
        "audit_wall_seconds": round(time.perf_counter() - started, 3),
        "audit_peak_rss_mb": get_peak_rss_mb(),
        "checks": checks,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for audit_warehouse.py."""
    parser = argparse.ArgumentParser(
        description="Run automated data-integrity checks on nfl_stats.duckdb."
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Override warehouse data directory",
    )
    parser.add_argument(
        "--profile",
        choices=VALID_PROFILES,
        default="core",
        help="Expected dataset profile (lite, core, full)",
    )
    parser.add_argument(
        "--telemetry-out",
        type=str,
        default=None,
        help="Write or merge machine-readable telemetry + audit JSON to this path",
    )
    parser.add_argument(
        "--allow-fixture-floors",
        action="store_true",
        help="Only require >0 rows instead of full historical row-count floors",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any integrity check fails",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for live warehouse audit."""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    args = parse_args(argv)
    paths = (
        WarehousePaths.for_data_dir(args.data_dir)
        if args.data_dir
        else WarehousePaths.default()
    )
    audit_report = run_audit(
        paths,
        profile=args.profile,
        enforce_full_floors=not args.allow_fixture_floors,
    )

    for chk in audit_report["checks"]:
        print(f"[{chk['status']}] {chk['name']}")
        if chk["name"] == "player_dim_integrity":
            active = chk.get("active_skill_coverage") or {}
            ids = active.get("ids") or {}
            if ids:
                yr = active.get("season")
                tot = active.get("total_skill_players")
                y_cov = ids.get("yahoo_id", {})
                m_cov = ids.get("mfl_id", {})
                s_cov = ids.get("sleeper_id", {})
                print(
                    f"  [INFO] Active {yr} skill players ({tot}): "
                    f"mfl_id={m_cov.get('pct')}% yahoo_id={y_cov.get('pct')}% "
                    f"(missing={y_cov.get('missing')}) sleeper_id={s_cov.get('pct')}%"
                )
            collisions = chk.get("secondary_id_collisions") or {}
            if collisions:
                print(f"  [INFO] Secondary ID collisions: {collisions}")
        elif chk["name"] == "scoring_reconciliation":
            print(
                f"  [INFO] PPR 0-diff vs nflverse: {chk['ppr_matched_rows']:,} matched, "
                f"{chk['ppr_diff_count']} diffs (max={chk['ppr_max_abs_diff']:.2e}), "
                f"K={chk['kicker_rows']:,}, DST={chk['dst_rows']:,}"
            )
        elif chk["name"] == "point_in_time_guardrails":
            print(
                f"  [INFO] depth_charts dt_pregame={chk['depth_charts_dt_pregame_rows']:,}, "
                f"lookahead={chk['depth_charts_lookahead_rows']}"
            )
        if chk["status"] != "PASS":
            print(f"  DETAILS: {json.dumps(chk)}")

    if args.telemetry_out:
        out_path = Path(args.telemetry_out)
        prev_wall = None
        prev_peak_rss = None
        if out_path.is_file():
            try:
                prev = json.loads(out_path.read_text(encoding="utf-8"))
                prev_wall = prev.get("wall_seconds")
                prev_peak_rss = prev.get("peak_rss_mb")
            except Exception:
                prev_wall = None
                prev_peak_rss = None
        telemetry = collect_warehouse_telemetry(
            paths,
            wall_seconds=prev_wall,
            peak_rss_mb=prev_peak_rss,
            profile=args.profile,
            audit_summary=audit_report,
        )
        write_telemetry(out_path, telemetry)
        print(f"Telemetry written to {out_path}")

    print(
        f"Audit result: {audit_report['overall_status']} "
        f"({audit_report['audit_wall_seconds']:.3f}s)"
    )
    if args.strict and audit_report["overall_status"] != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
