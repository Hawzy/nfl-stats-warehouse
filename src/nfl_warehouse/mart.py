"""Point-in-time guardrail views and canonical team mappings in mart.*."""

from typing import Callable

import duckdb

__all__ = ["TEAM_ABBR_ROWS", "build_mart_views", "canon_team_sql"]

_CURRENT_CANON_TEAMS: tuple[str, ...] = (
    "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE",
    "DAL", "DEN", "DET", "GB", "HOU", "IND", "JAX", "KC",
    "LA", "LAC", "LV", "MIA", "MIN", "NE", "NO", "NYG",
    "NYJ", "PHI", "PIT", "SEA", "SF", "TB", "TEN", "WAS",
)

# (raw_abbr, canonical_team, franchise_abbr, is_historical)
TEAM_ABBR_ROWS: tuple[tuple[str, str, str, bool], ...] = (
    *((abbr, abbr, abbr, False) for abbr in _CURRENT_CANON_TEAMS),
    ("LAR", "LA", "LA", False),
    ("JAC", "JAX", "JAX", False),
    ("WSH", "WAS", "WAS", False),
    ("STL", "LA", "LA", True),
    ("SL", "LA", "LA", True),
    ("SD", "LAC", "LAC", True),
    ("OAK", "LV", "LV", True),
    ("ARZ", "ARI", "ARI", True),
    ("BLT", "BAL", "BAL", True),
    ("CLV", "CLE", "CLE", True),
    ("HST", "HOU", "HOU", True),
)

_ALIAS_MAP: dict[str, str] = {raw: canon for raw, canon, _, _ in TEAM_ABBR_ROWS}


def canon_team_sql(col_expr: str) -> str:
    """Return an inline SQL CASE expression normalizing team abbreviations."""
    cases = " ".join(
        f"WHEN upper(trim(CAST({col_expr} AS VARCHAR))) = '{raw}' THEN '{canon}'"
        for raw, canon, _, _ in TEAM_ABBR_ROWS
        if raw != canon
    )
    return f"CASE {cases} ELSE upper(trim(CAST({col_expr} AS VARCHAR))) END"


def _describe_cols(con: duckdb.DuckDBPyConnection, view_name: str) -> set[str] | None:
    """Return lowercase column names of a DuckDB table/view, or None if missing."""
    try:
        rows = con.execute(f"DESCRIBE {view_name}").fetchall()
    except duckdb.Error:
        return None
    return {row[0].lower() for row in rows}


def _build_team_abbr_map(con: duckdb.DuckDBPyConnection) -> None:
    """Create or replace mart.team_abbr_map lookup table."""
    con.execute(
        """
        CREATE OR REPLACE TABLE mart.team_abbr_map (
            raw_abbr VARCHAR PRIMARY KEY,
            team VARCHAR NOT NULL,
            franchise_abbr VARCHAR NOT NULL,
            is_historical BOOLEAN NOT NULL
        )
        """
    )
    con.executemany(
        "INSERT INTO mart.team_abbr_map (raw_abbr, team, franchise_abbr, is_historical) "
        "VALUES (?, ?, ?, ?)",
        TEAM_ABBR_ROWS,
    )


def _build_nextgen_weekly_views(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None],
) -> list[str]:
    """Create mart.nextgen_*_weekly views excluding week = 0 rollups and normalizing team."""
    created: list[str] = []
    for ds_name in ("nextgen_passing", "nextgen_rushing", "nextgen_receiving"):
        cols = _describe_cols(con, f"nfl.{ds_name}")
        if cols is None:
            log(f"Skipping mart.{ds_name}_weekly: nfl.{ds_name} missing")
            continue

        raw_team_col = "ng.team_abbr" if "team_abbr" in cols else ("ng.team" if "team" in cols else None)
        if raw_team_col is not None and "team" not in cols:
            team_select = f"COALESCE(m.team, {raw_team_col}) AS team, "
            join_clause = (
                f"LEFT JOIN mart.team_abbr_map m "
                f"ON upper(trim(CAST({raw_team_col} AS VARCHAR))) = m.raw_abbr"
            )
            select_prefix = f"{team_select}ng.*"
        elif raw_team_col is not None:
            team_select = f"COALESCE(m.team, {raw_team_col}) AS team, "
            join_clause = (
                f"LEFT JOIN mart.team_abbr_map m "
                f"ON upper(trim(CAST({raw_team_col} AS VARCHAR))) = m.raw_abbr"
            )
            select_prefix = f"{team_select}ng.* EXCLUDE (team)"
        else:
            join_clause = ""
            select_prefix = "ng.*"

        view_name = f"{ds_name}_weekly"
        con.execute(
            f"""
            CREATE OR REPLACE VIEW mart.{view_name} AS
            SELECT {select_prefix}
            FROM nfl.{ds_name} ng
            {join_clause}
            WHERE TRY_CAST(ng.week AS INT) >= 1
            """
        )
        created.append(view_name)
    return created


def _build_player_team_week_view(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None],
) -> bool:
    """Create mart.player_team_week deduplicated 1 row per (season, week, gsis_id)."""
    cols = _describe_cols(con, "nfl.rosters_weekly")
    if cols is None:
        log("Skipping mart.player_team_week: nfl.rosters_weekly missing")
        return False

    depth_pos = "rw.depth_chart_position" if "depth_chart_position" in cols else "CAST(NULL AS VARCHAR)"
    full_name = "rw.full_name" if "full_name" in cols else "CAST(NULL AS VARCHAR)"
    status_col = "rw.status" if "status" in cols else "CAST(NULL AS VARCHAR)"
    game_type_col = "rw.game_type" if "game_type" in cols else "'REG'"
    jersey_col = "rw.jersey_number" if "jersey_number" in cols else "CAST(NULL AS VARCHAR)"

    con.execute(
        f"""
        CREATE OR REPLACE VIEW mart.player_team_week AS
        SELECT
            CAST(rw.season AS INT) AS season,
            TRY_CAST(rw.week AS INT) AS week,
            trim(CAST(rw.gsis_id AS VARCHAR)) AS gsis_id,
            COALESCE(m.team, rw.team) AS team,
            rw.team AS raw_team,
            CAST(rw.position AS VARCHAR) AS position,
            CAST({depth_pos} AS VARCHAR) AS depth_chart_position,
            CAST({full_name} AS VARCHAR) AS full_name,
            CAST({status_col} AS VARCHAR) AS status,
            CAST({game_type_col} AS VARCHAR) AS game_type,
            CAST({jersey_col} AS VARCHAR) AS jersey_number
        FROM nfl.rosters_weekly rw
        LEFT JOIN mart.team_abbr_map m
          ON upper(trim(CAST(rw.team AS VARCHAR))) = m.raw_abbr
        WHERE rw.gsis_id IS NOT NULL
          AND trim(CAST(rw.gsis_id AS VARCHAR)) != ''
          AND TRY_CAST(rw.week AS INT) >= 1
        QUALIFY row_number() OVER (
            PARTITION BY CAST(rw.season AS INT), TRY_CAST(rw.week AS INT), trim(CAST(rw.gsis_id AS VARCHAR))
            ORDER BY
                ({status_col} = 'ACT') DESC NULLS LAST,
                ({game_type_col} = 'REG') DESC NULLS LAST,
                COALESCE(m.team, rw.team) ASC NULLS LAST
        ) = 1
        """
    )
    return True


def _build_depth_charts_weekly_view(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None],
) -> bool:
    """Create mart.depth_charts_weekly bridging pre-2025 weekly and 2025+ dt snapshots."""
    dc_cols = _describe_cols(con, "nfl.depth_charts")
    if dc_cols is None:
        log("Skipping mart.depth_charts_weekly: nfl.depth_charts missing")
        return False

    sched_cols = _describe_cols(con, "nfl.schedules")
    has_legacy = {"season", "week"}.issubset(dc_cols)
    has_dt = "dt" in dc_cols and sched_cols is not None

    branches: list[str] = []

    if has_legacy:
        if "club_code" in dc_cols and "team" in dc_cols:
            leg_team_col = "coalesce(dc.club_code, dc.team)"
        elif "club_code" in dc_cols:
            leg_team_col = "dc.club_code"
        else:
            leg_team_col = "dc.team"

        if "full_name" in dc_cols and "player_name" in dc_cols:
            leg_name = "coalesce(dc.full_name, dc.player_name)"
        elif "full_name" in dc_cols:
            leg_name = "dc.full_name"
        elif "player_name" in dc_cols:
            leg_name = "dc.player_name"
        else:
            leg_name = "CAST(NULL AS VARCHAR)"

        if "position" in dc_cols and "pos_abb" in dc_cols:
            leg_pos = "coalesce(dc.position, dc.pos_abb)"
        elif "position" in dc_cols:
            leg_pos = "dc.position"
        elif "pos_abb" in dc_cols:
            leg_pos = "dc.pos_abb"
        else:
            leg_pos = "CAST(NULL AS VARCHAR)"

        if "depth_team" in dc_cols and "pos_rank" in dc_cols:
            leg_rank = "coalesce(TRY_CAST(dc.depth_team AS INT), TRY_CAST(dc.pos_rank AS INT))"
        elif "depth_team" in dc_cols:
            leg_rank = "TRY_CAST(dc.depth_team AS INT)"
        elif "pos_rank" in dc_cols:
            leg_rank = "TRY_CAST(dc.pos_rank AS INT)"
        else:
            leg_rank = "CAST(NULL AS INT)"

        leg_gt = "dc.game_type" if "game_type" in dc_cols else "'REG'"
        leg_formation = "dc.formation" if "formation" in dc_cols else "CAST(NULL AS VARCHAR)"

        branches.append(
            f"""
            SELECT
                CAST(dc.season AS INT) AS season,
                CAST(dc.week AS INT) AS week,
                CAST({leg_gt} AS VARCHAR) AS game_type,
                CAST(NULL AS TIMESTAMPTZ) AS snapshot_dt,
                COALESCE(m.team, {leg_team_col}) AS team,
                CAST(dc.gsis_id AS VARCHAR) AS gsis_id,
                CAST({leg_name} AS VARCHAR) AS player_name,
                CAST({leg_pos} AS VARCHAR) AS position,
                {leg_rank} AS depth_rank,
                CAST({leg_formation} AS VARCHAR) AS formation,
                CAST('legacy_weekly' AS VARCHAR) AS source_format
            FROM nfl.depth_charts dc
            LEFT JOIN mart.team_abbr_map m
              ON upper(trim(CAST({leg_team_col} AS VARCHAR))) = m.raw_abbr
            WHERE dc.season IS NOT NULL AND dc.week IS NOT NULL
            """
        )

    if has_dt:
        dt_team_col = "dc.team" if "team" in dc_cols else "dc.club_code"
        if "player_name" in dc_cols and "full_name" in dc_cols:
            dt_name = "coalesce(dc.player_name, dc.full_name)"
        elif "player_name" in dc_cols:
            dt_name = "dc.player_name"
        elif "full_name" in dc_cols:
            dt_name = "dc.full_name"
        else:
            dt_name = "CAST(NULL AS VARCHAR)"

        if "pos_abb" in dc_cols and "position" in dc_cols:
            dt_pos = "coalesce(dc.pos_abb, dc.position)"
        elif "pos_abb" in dc_cols:
            dt_pos = "dc.pos_abb"
        elif "position" in dc_cols:
            dt_pos = "dc.position"
        else:
            dt_pos = "CAST(NULL AS VARCHAR)"

        if "pos_rank" in dc_cols and "depth_team" in dc_cols:
            dt_rank = "coalesce(TRY_CAST(dc.pos_rank AS INT), TRY_CAST(dc.depth_team AS INT))"
        elif "pos_rank" in dc_cols:
            dt_rank = "TRY_CAST(dc.pos_rank AS INT)"
        elif "depth_team" in dc_cols:
            dt_rank = "TRY_CAST(dc.depth_team AS INT)"
        else:
            dt_rank = "CAST(NULL AS INT)"

        if "pos_grp" in dc_cols and "formation" in dc_cols:
            dt_formation = "coalesce(dc.pos_grp, dc.formation)"
        elif "pos_grp" in dc_cols:
            dt_formation = "dc.pos_grp"
        elif "formation" in dc_cols:
            dt_formation = "dc.formation"
        else:
            dt_formation = "CAST(NULL AS VARCHAR)"

        sched_gt = "s.game_type" if "game_type" in sched_cols else "'REG'"
        kickoff_expr = (
            "(TRY_CAST(s.gameday AS DATE) + COALESCE(TRY_CAST(s.gametime AS TIME), TIME '09:00:00')) "
            "AT TIME ZONE 'America/New_York'"
            if "gametime" in sched_cols
            else "(TRY_CAST(s.gameday AS DATE) + TIME '09:00:00') AT TIME ZONE 'America/New_York'"
        )
        cte_block = f"""
        WITH sched_teams AS (
            SELECT
                CAST(s.season AS INT) AS season,
                CAST(s.week AS INT) AS week,
                CAST({sched_gt} AS VARCHAR) AS game_type,
                COALESCE(m.team, s.home_team) AS team,
                {kickoff_expr} AS kickoff_ts
            FROM nfl.schedules s
            LEFT JOIN mart.team_abbr_map m
              ON upper(trim(CAST(s.home_team AS VARCHAR))) = m.raw_abbr
            WHERE s.gameday IS NOT NULL
            UNION ALL
            SELECT
                CAST(s.season AS INT) AS season,
                CAST(s.week AS INT) AS week,
                CAST({sched_gt} AS VARCHAR) AS game_type,
                COALESCE(m.team, s.away_team) AS team,
                {kickoff_expr} AS kickoff_ts
            FROM nfl.schedules s
            LEFT JOIN mart.team_abbr_map m
              ON upper(trim(CAST(s.away_team AS VARCHAR))) = m.raw_abbr
            WHERE s.gameday IS NOT NULL
        ),
        sched_dedup AS (
            SELECT season, week, game_type, team, kickoff_ts
            FROM sched_teams
            WHERE kickoff_ts IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY season, week, team ORDER BY kickoff_ts ASC) = 1
        ),
        dt_snapshots AS (
            SELECT DISTINCT
                COALESCE(m.team, {dt_team_col}) AS team,
                {dt_team_col} AS raw_team,
                dc.dt AS raw_dt,
                TRY_CAST(dc.dt AS TIMESTAMPTZ) AS dt_ts
            FROM nfl.depth_charts dc
            LEFT JOIN mart.team_abbr_map m
              ON upper(trim(CAST({dt_team_col} AS VARCHAR))) = m.raw_abbr
            WHERE dc.dt IS NOT NULL AND TRY_CAST(dc.dt AS TIMESTAMPTZ) IS NOT NULL
        ),
        matched_snapshot AS (
            SELECT
                s.season,
                s.week,
                s.game_type,
                s.team,
                d.raw_team,
                d.raw_dt,
                d.dt_ts
            FROM sched_dedup s
            JOIN dt_snapshots d
              ON d.team = s.team
             AND d.dt_ts < s.kickoff_ts
             AND d.dt_ts >= s.kickoff_ts - INTERVAL 14 DAY
            QUALIFY row_number() OVER (
                PARTITION BY s.season, s.week, s.team
                ORDER BY d.dt_ts DESC
            ) = 1
        )
        """
        branches.append(
            f"""
            SELECT
                ms.season,
                ms.week,
                ms.game_type,
                ms.dt_ts AS snapshot_dt,
                ms.team,
                CAST(dc.gsis_id AS VARCHAR) AS gsis_id,
                CAST({dt_name} AS VARCHAR) AS player_name,
                CAST({dt_pos} AS VARCHAR) AS position,
                {dt_rank} AS depth_rank,
                CAST({dt_formation} AS VARCHAR) AS formation,
                CAST('dt_pregame' AS VARCHAR) AS source_format
            FROM matched_snapshot ms
            JOIN nfl.depth_charts dc
              ON dc.dt = ms.raw_dt
             AND {dt_team_col} = ms.raw_team
            WHERE dc.dt IS NOT NULL
            """
        )
    else:
        cte_block = ""

    if not branches:
        log("Skipping mart.depth_charts_weekly: no supported depth chart columns or schedules found")
        return False

    body = "\nUNION ALL\n".join(b.strip() for b in branches)
    prefix = f"{cte_block.strip()}\n" if cte_block else ""
    con.execute(f"CREATE OR REPLACE VIEW mart.depth_charts_weekly AS\n{prefix}{body}")
    return True


def build_mart_views(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None] = print,
) -> list[str]:
    """Create schema mart with point-in-time backtesting guardrail tables and views."""
    con.execute("CREATE SCHEMA IF NOT EXISTS mart")
    _build_team_abbr_map(con)
    created: list[str] = ["team_abbr_map"]

    created.extend(_build_nextgen_weekly_views(con, log))
    if _build_player_team_week_view(con, log):
        created.append("player_team_week")
    if _build_depth_charts_weekly_view(con, log):
        created.append("depth_charts_weekly")

    return created
