"""Materialized DuckDB Gold tables for league-agnostic advanced stats, xFP components, and DvP."""

from __future__ import annotations

import math
from typing import Callable

import duckdb

__all__ = [
    "EMPIRICAL_FUMBLE_LOST_RATE",
    "FROZEN_ROUTE_BETAS",
    "SKILL_POSITIONS",
    "build_advanced_stats_tables",
    "build_player_week_adv",
    "build_player_week_xfp",
    "build_team_week_dvp",
    "impute_constrained_routes",
]

SKILL_POSITIONS = ("QB", "RB", "WR", "TE")

# Frozen pre-2025 Physical-Bound Fractional Route Model coefficients fitted on 8,874 player-weeks
# of 2023-2024 nfl.participation + nfl.pbp + nfl.snap_counts + nfl.ftn_charting (quarantined in-season):
# logit(r_frac) = beta_0 + beta_1*SnapPct + beta_2*CarryShare + beta_3*(aDOT/10) + beta_4*PlayActionPct
FROZEN_ROUTE_BETAS: dict[str, tuple[float, float, float, float, float]] = {
    "WR": (-2.6298, 5.4429, -0.5707, -0.0158, 0.0553),
    "TE": (-2.7430, 4.2554, 0.6002, 0.0691, -0.2703),
    "RB": (-2.4623, 4.9807, -2.1602, -0.0713, -0.0159),
}

# Pre-2026 empirical positional fumble-lost rate per touch/sack on 2023-2025 nfl.player_stats_week
EMPIRICAL_FUMBLE_LOST_RATE: dict[str, float] = {
    "QB": 0.032904,
    "RB": 0.004416,
    "WR": 0.005069,
    "TE": 0.006225,
}

_TEAM_NORMALIZE_CASES = (
    ("LAR", "LA"),
    ("STL", "LA"),
    ("SL", "LA"),
    ("WSH", "WAS"),
    ("JAC", "JAX"),
    ("GBP", "GB"),
    ("KCC", "KC"),
    ("NEP", "NE"),
    ("NOS", "NO"),
    ("SFO", "SF"),
    ("TBB", "TB"),
    ("LVR", "LV"),
    ("OAK", "LV"),
    ("SD", "LAC"),
    ("ARZ", "ARI"),
    ("BLT", "BAL"),
    ("CLV", "CLE"),
    ("HST", "HOU"),
)


def impute_constrained_routes(
    pos: str,
    team_db: float,
    off_snaps: float,
    snap_pct: float,
    carries: float,
    team_carries: float,
    targets: float,
    adot_prior: float,
    pa_pct: float,
) -> float:
    """Physical-Bound Constrained Fractional Route Model evaluated without participation tables."""
    if team_db <= 0.0 or off_snaps <= 0.0:
        return float(targets)
    b0, b1, b2, b3, b4 = FROZEN_ROUTE_BETAS.get(pos, FROZEN_ROUTE_BETAS["WR"])
    c_share = carries / max(1.0, team_carries)
    adot_clamped = max(-5.0, min(25.0, adot_prior))
    logit_val = b0 + b1 * snap_pct + b2 * c_share + b3 * (adot_clamped / 10.0) + b4 * pa_pct
    frac = 1.0 / (1.0 + math.exp(-max(-10.0, min(10.0, logit_val))))
    lb = float(targets)
    ub = float(min(max(targets, off_snaps - carries), team_db))
    return min(ub, max(lb, team_db * frac))


def _rel_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 0")
        return True
    except duckdb.Error:
        return False


def _table_cols(con: duckdb.DuckDBPyConnection, name: str) -> set[str]:
    if not _rel_exists(con, name):
        return set()
    return {str(r[0]).lower() for r in con.execute(f"DESCRIBE {name}").fetchall()}


def _sql_norm_team(expr: str) -> str:
    cases = " ".join(f"WHEN '{src}' THEN '{dst}'" for src, dst in _TEAM_NORMALIZE_CASES)
    return (
        f"CASE UPPER(TRIM(COALESCE(CAST({expr} AS VARCHAR), ''))) "
        f"{cases} ELSE UPPER(TRIM(COALESCE(CAST({expr} AS VARCHAR), ''))) END"
    )


def _create_empty_adv_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        """
        CREATE OR REPLACE TABLE main.player_week_adv (
            gsis_id VARCHAR,
            player_name VARCHAR,
            position VARCHAR,
            team VARCHAR,
            opponent_team VARCHAR,
            season INTEGER,
            week INTEGER,
            team_dropbacks DOUBLE,
            eff_dropbacks DOUBLE,
            team_carries DOUBLE,
            offense_snaps DOUBLE,
            snap_pct DOUBLE,
            attempts DOUBLE,
            sacks_suffered DOUBLE,
            pass_dropbacks DOUBLE,
            carries DOUBLE,
            targets DOUBLE,
            receptions DOUBLE,
            receiving_yards DOUBLE,
            passing_yards DOUBLE,
            passing_air_yards DOUBLE,
            receiving_air_yards DOUBLE,
            target_share DOUBLE,
            air_yards_share DOUBLE,
            wopr DOUBLE,
            est_routes DOUBLE,
            constrained_routes DOUBLE,
            yprr DOUBLE,
            yprr_constrained DOUBLE,
            tprr_pct DOUBLE,
            rec_adot DOUBLE,
            ngs_rush_attempts DOUBLE,
            ryoe DOUBLE,
            ryoe_per_att DOUBLE,
            passing_epa DOUBLE,
            epa_per_db DOUBLE,
            passing_cpoe DOUBLE,
            pass_adot DOUBLE,
            ff_xfp_raw DOUBLE
        )
        """
    )


def _create_empty_xfp_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        """
        CREATE OR REPLACE TABLE main.player_week_xfp (
            gsis_id VARCHAR,
            player_name VARCHAR,
            position VARCHAR,
            team VARCHAR,
            opponent_team VARCHAR,
            season INTEGER,
            week INTEGER,
            has_ff_opp BOOLEAN,
            pass_yds_exp DOUBLE,
            pass_td_exp DOUBLE,
            pass_int_exp DOUBLE,
            rush_yds_exp DOUBLE,
            rush_td_exp DOUBLE,
            rush_attempts_exp DOUBLE,
            rec_yds_exp DOUBLE,
            receptions_exp DOUBLE,
            rec_td_exp DOUBLE,
            two_pt_exp DOUBLE,
            sacks_suffered DOUBLE,
            passing_yards DOUBLE,
            passing_tds DOUBLE,
            passing_interceptions DOUBLE,
            rushing_yards DOUBLE,
            rushing_tds DOUBLE,
            receptions DOUBLE,
            receiving_yards DOUBLE,
            receiving_tds DOUBLE,
            fumbles_lost DOUBLE,
            two_pt_actual DOUBLE,
            xfp_ppr DOUBLE,
            xfp_half DOUBLE,
            xfp_std DOUBLE,
            xfp_yahoo DOUBLE,
            actual_ppr DOUBLE,
            actual_half DOUBLE,
            actual_std DOUBLE,
            actual_yahoo DOUBLE,
            luck_delta_ppr DOUBLE,
            luck_delta_half DOUBLE,
            luck_delta_std DOUBLE,
            luck_delta_yahoo DOUBLE,
            ff_total_xfp DOUBLE
        )
        """
    )


def _create_empty_dvp_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        """
        CREATE OR REPLACE TABLE main.team_week_dvp (
            defense_team VARCHAR,
            offense_team VARCHAR,
            position VARCHAR,
            season INTEGER,
            week INTEGER,
            player_count INTEGER,
            pts_allowed_ppr DOUBLE,
            pts_allowed_half DOUBLE,
            pts_allowed_std DOUBLE,
            pts_allowed_yahoo DOUBLE,
            xfp_allowed_ppr DOUBLE,
            xfp_allowed_half DOUBLE,
            xfp_allowed_std DOUBLE,
            xfp_allowed_yahoo DOUBLE
        )
        """
    )


def _psw_col(cols: set[str], name: str, alias: str = "s") -> str:
    return (
        f"COALESCE(CAST({alias}.{name} AS DOUBLE), CAST(0.0 AS DOUBLE))"
        if name.lower() in cols
        else "CAST(0.0 AS DOUBLE)"
    )


def _psw_team_expr(cols: set[str], alias: str = "s") -> str:
    if "team" in cols and "recent_team" in cols:
        return _sql_norm_team(f"COALESCE({alias}.team, {alias}.recent_team)")
    if "team" in cols:
        return _sql_norm_team(f"{alias}.team")
    if "recent_team" in cols:
        return _sql_norm_team(f"{alias}.recent_team")
    return "CAST('' AS VARCHAR)"


def _psw_name_expr(cols: set[str], alias: str = "s") -> str:
    if "player_display_name" in cols and "player_name" in cols:
        return (
            f"COALESCE(CAST({alias}.player_display_name AS VARCHAR), "
            f"CAST({alias}.player_name AS VARCHAR), CAST({alias}.player_id AS VARCHAR))"
        )
    if "player_display_name" in cols:
        return f"COALESCE(CAST({alias}.player_display_name AS VARCHAR), CAST({alias}.player_id AS VARCHAR))"
    if "player_name" in cols:
        return f"COALESCE(CAST({alias}.player_name AS VARCHAR), CAST({alias}.player_id AS VARCHAR))"
    return f"CAST({alias}.player_id AS VARCHAR)"


def build_player_week_adv(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None] = print,
) -> int:
    """Materialize main.player_week_adv across REG weeks for QB/RB/WR/TE."""
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    if not _rel_exists(con, "nfl.player_stats_week"):
        _create_empty_adv_table(con)
        log("nfl.player_stats_week missing; created empty main.player_week_adv")
        return 0

    psw_cols = _table_cols(con, "nfl.player_stats_week")
    if not {"player_id", "position", "season", "week"}.issubset(psw_cols):
        _create_empty_adv_table(con)
        log("nfl.player_stats_week missing required columns; created empty main.player_week_adv")
        return 0

    pbp_cols = _table_cols(con, "nfl.pbp")
    has_pbp = {"season", "week", "posteam", "qb_dropback"}.issubset(pbp_cols)
    snap_cols = _table_cols(con, "nfl.snap_counts")
    dim_cols = _table_cols(con, "main.player_dim")
    has_snaps = {"pfr_player_id", "season", "week"}.issubset(snap_cols) and {
        "gsis_id",
        "pfr_id",
    }.issubset(dim_cols)
    opp_cols = _table_cols(con, "nfl.ff_opportunity")
    has_opp = {"player_id", "season", "week"}.issubset(opp_cols)
    ngs_cols = _table_cols(con, "nfl.nextgen_rushing")
    has_ngs = {"player_gsis_id", "season", "week"}.issubset(ngs_cols)

    psw_st_filter = (
        "AND UPPER(TRIM(CAST(s.season_type AS VARCHAR))) = 'REG'"
        if "season_type" in psw_cols
        else ""
    )

    tm_expr = _psw_team_expr(psw_cols, "s")
    opp_tm_expr = _sql_norm_team("s.opponent_team") if "opponent_team" in psw_cols else "CAST('' AS VARCHAR)"
    name_expr = _psw_name_expr(psw_cols, "s")
    sacks_expr = _psw_col(psw_cols, "sacks_suffered")
    att_expr = _psw_col(psw_cols, "attempts")
    car_expr = _psw_col(psw_cols, "carries")
    tgt_expr = _psw_col(psw_cols, "targets")
    rec_expr = _psw_col(psw_cols, "receptions")
    rec_yds_expr = _psw_col(psw_cols, "receiving_yards")
    pass_yds_expr = _psw_col(psw_cols, "passing_yards")
    pass_air_expr = _psw_col(psw_cols, "passing_air_yards")
    rec_air_expr = _psw_col(psw_cols, "receiving_air_yards")
    tgt_sh_expr = _psw_col(psw_cols, "target_share")
    air_sh_expr = _psw_col(psw_cols, "air_yards_share")
    wopr_expr = _psw_col(psw_cols, "wopr")
    epa_expr = _psw_col(psw_cols, "passing_epa")
    cpoe_expr = _psw_col(psw_cols, "passing_cpoe")

    if has_pbp:
        pbp_st_filter = (
            "AND UPPER(TRIM(CAST(season_type AS VARCHAR))) = 'REG'"
            if "season_type" in pbp_cols
            else ""
        )
        team_db_cte = f"""
        team_db AS (
            SELECT
                CAST(season AS INTEGER) AS season,
                CAST(week AS INTEGER) AS week,
                {_sql_norm_team('posteam')} AS team,
                CAST(SUM(CASE WHEN COALESCE(CAST(qb_dropback AS DOUBLE), 0.0) = 1.0 THEN 1.0 ELSE 0.0 END) AS DOUBLE) AS dropbacks
            FROM nfl.pbp
            WHERE posteam IS NOT NULL
              AND TRY_CAST(week AS INTEGER) >= 1
              {pbp_st_filter}
            GROUP BY 1, 2, 3
        )
        """
        team_db_val = "CAST(COALESCE(db.dropbacks, 0.0) AS DOUBLE)"
        eff_db_val = "CAST(COALESCE(db.dropbacks, 34.0) AS DOUBLE)"
    else:
        team_db_cte = (
            "team_db AS ("
            "SELECT CAST(0 AS INTEGER) AS season, CAST(0 AS INTEGER) AS week, "
            "CAST('' AS VARCHAR) AS team, CAST(0.0 AS DOUBLE) AS dropbacks WHERE FALSE)"
        )
        team_db_val = "CAST(0.0 AS DOUBLE)"
        eff_db_val = "CAST(COALESCE(NULLIF(tc.psw_dropbacks, 0.0), 34.0) AS DOUBLE)"

    if has_snaps:
        snap_gt = (
            "AND UPPER(TRIM(CAST(game_type AS VARCHAR))) = 'REG'"
            if "game_type" in snap_cols
            else ""
        )
        snap_off = (
            "COALESCE(CAST(offense_snaps AS DOUBLE), CAST(0.0 AS DOUBLE))"
            if "offense_snaps" in snap_cols
            else "CAST(0.0 AS DOUBLE)"
        )
        snap_pct_col = (
            "COALESCE(CAST(offense_pct AS DOUBLE), CAST(0.0 AS DOUBLE))"
            if "offense_pct" in snap_cols
            else "CAST(0.0 AS DOUBLE)"
        )
        snap_cte = f"""
        dim_pfr AS (
            SELECT CAST(gsis_id AS VARCHAR) AS gsis_id, ANY_VALUE(CAST(pfr_id AS VARCHAR)) AS pfr_id
            FROM main.player_dim
            WHERE gsis_id IS NOT NULL
              AND TRIM(CAST(gsis_id AS VARCHAR)) != ''
              AND pfr_id IS NOT NULL
              AND TRIM(CAST(pfr_id AS VARCHAR)) != ''
            GROUP BY 1
        ),
        snaps_dedup AS (
            SELECT
                CAST(pfr_player_id AS VARCHAR) AS pfr_player_id,
                CAST(season AS INTEGER) AS season,
                CAST(week AS INTEGER) AS week,
                {snap_off} AS offense_snaps,
                {snap_pct_col} AS offense_pct
            FROM nfl.snap_counts
            WHERE pfr_player_id IS NOT NULL
              AND TRY_CAST(week AS INTEGER) >= 1
              {snap_gt}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(pfr_player_id AS VARCHAR), CAST(season AS INTEGER), CAST(week AS INTEGER)
                ORDER BY {snap_off} DESC, {snap_pct_col} DESC
            ) = 1
        )
        """
        snap_join = """
        LEFT JOIN dim_pfr dp ON dp.gsis_id = CAST(s.player_id AS VARCHAR)
        LEFT JOIN snaps_dedup sc
          ON sc.pfr_player_id = dp.pfr_id
         AND sc.season = CAST(s.season AS INTEGER)
         AND sc.week = CAST(s.week AS INTEGER)
        """
        off_snaps_expr = "COALESCE(sc.offense_snaps, CAST(0.0 AS DOUBLE))"
        snap_pct_expr = "COALESCE(sc.offense_pct, CAST(0.0 AS DOUBLE))"
    else:
        snap_cte = ""
        snap_join = ""
        off_snaps_expr = "CAST(0.0 AS DOUBLE)"
        snap_pct_expr = "CAST(0.0 AS DOUBLE)"

    if has_opp:
        opp_st_filter = (
            "AND UPPER(TRIM(CAST(season_type AS VARCHAR))) = 'REG'"
            if "season_type" in opp_cols
            else ""
        )
        opp_tot_col = (
            "COALESCE(CAST(total_fantasy_points_exp AS DOUBLE), CAST(0.0 AS DOUBLE))"
            if "total_fantasy_points_exp" in opp_cols
            else "CAST(0.0 AS DOUBLE)"
        )
        opp_cte = f"""
        opp_dedup AS (
            SELECT
                CAST(player_id AS VARCHAR) AS player_id,
                CAST(season AS INTEGER) AS season,
                CAST(week AS INTEGER) AS week,
                {opp_tot_col} AS total_fantasy_points_exp
            FROM nfl.ff_opportunity
            WHERE player_id IS NOT NULL
              AND TRIM(CAST(player_id AS VARCHAR)) != ''
              AND TRY_CAST(week AS INTEGER) >= 1
              {opp_st_filter}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(player_id AS VARCHAR), CAST(season AS INTEGER), CAST(week AS INTEGER)
                ORDER BY {opp_tot_col} DESC
            ) = 1
        )
        """
        opp_join = """
        LEFT JOIN opp_dedup o
          ON o.player_id = CAST(s.player_id AS VARCHAR)
         AND o.season = CAST(s.season AS INTEGER)
         AND o.week = CAST(s.week AS INTEGER)
        """
        ff_xfp_expr = "COALESCE(o.total_fantasy_points_exp, CAST(0.0 AS DOUBLE))"
    else:
        opp_cte = ""
        opp_join = ""
        ff_xfp_expr = "CAST(0.0 AS DOUBLE)"

    if has_ngs:
        ngs_st = (
            "AND UPPER(TRIM(CAST(season_type AS VARCHAR))) = 'REG'"
            if "season_type" in ngs_cols
            else ""
        )
        ngs_att_col = (
            "COALESCE(CAST(rush_attempts AS DOUBLE), CAST(0.0 AS DOUBLE))"
            if "rush_attempts" in ngs_cols
            else "CAST(0.0 AS DOUBLE)"
        )
        ngs_ryoe_col = (
            "COALESCE(CAST(rush_yards_over_expected AS DOUBLE), CAST(0.0 AS DOUBLE))"
            if "rush_yards_over_expected" in ngs_cols
            else "CAST(0.0 AS DOUBLE)"
        )
        ngs_cte = f"""
        ngs_dedup AS (
            SELECT
                CAST(player_gsis_id AS VARCHAR) AS player_gsis_id,
                CAST(season AS INTEGER) AS season,
                CAST(week AS INTEGER) AS week,
                {ngs_att_col} AS rush_attempts,
                {ngs_ryoe_col} AS ryoe
            FROM nfl.nextgen_rushing
            WHERE player_gsis_id IS NOT NULL
              AND TRY_CAST(week AS INTEGER) >= 1
              {ngs_st}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(player_gsis_id AS VARCHAR), CAST(season AS INTEGER), CAST(week AS INTEGER)
                ORDER BY {ngs_att_col} DESC
            ) = 1
        )
        """
        ngs_join = """
        LEFT JOIN ngs_dedup nr
          ON nr.player_gsis_id = CAST(s.player_id AS VARCHAR)
         AND nr.season = CAST(s.season AS INTEGER)
         AND nr.week = CAST(s.week AS INTEGER)
        """
        ngs_att_expr = "COALESCE(nr.rush_attempts, CAST(0.0 AS DOUBLE))"
        ryoe_expr = "COALESCE(nr.ryoe, CAST(0.0 AS DOUBLE))"
    else:
        ngs_cte = ""
        ngs_join = ""
        ngs_att_expr = "CAST(0.0 AS DOUBLE)"
        ryoe_expr = "CAST(0.0 AS DOUBLE)"

    # Build constrained route expression mirroring FROZEN_ROUTE_BETAS & impute_constrained_routes
    b_wr = FROZEN_ROUTE_BETAS["WR"]
    b_te = FROZEN_ROUTE_BETAS["TE"]
    b_rb = FROZEN_ROUTE_BETAS["RB"]
    adot_prior = (
        "(CASE WHEN b.targets > 0.0 THEN (b.receiving_air_yards / b.targets) "
        "WHEN b.position = 'WR' THEN 10.5 ELSE 7.0 END)"
    )
    adot_clamped = f"LEAST(25.0, GREATEST(-5.0, {adot_prior}))"
    c_share = "(b.carries / GREATEST(1.0, b.team_carries))"
    b0_sql = f"(CASE b.position WHEN 'WR' THEN {b_wr[0]} WHEN 'TE' THEN {b_te[0]} ELSE {b_rb[0]} END)"
    b1_sql = f"(CASE b.position WHEN 'WR' THEN {b_wr[1]} WHEN 'TE' THEN {b_te[1]} ELSE {b_rb[1]} END)"
    b2_sql = f"(CASE b.position WHEN 'WR' THEN {b_wr[2]} WHEN 'TE' THEN {b_te[2]} ELSE {b_rb[2]} END)"
    b3_sql = f"(CASE b.position WHEN 'WR' THEN {b_wr[3]} WHEN 'TE' THEN {b_te[3]} ELSE {b_rb[3]} END)"
    b4_sql = f"(CASE b.position WHEN 'WR' THEN {b_wr[4]} WHEN 'TE' THEN {b_te[4]} ELSE {b_rb[4]} END)"
    logit_sql = (
        f"({b0_sql} + {b1_sql} * b.snap_pct + {b2_sql} * {c_share} + "
        f"{b3_sql} * ({adot_clamped} / 10.0) + {b4_sql} * 0.22)"
    )
    frac_sql = f"(1.0 / (1.0 + EXP(-LEAST(10.0, GREATEST(-10.0, {logit_sql})))))"
    ub_sql = "LEAST(GREATEST(b.targets, b.offense_snaps - b.carries), b.eff_dropbacks)"
    constrained_routes_sql = f"""
        CAST(
            CASE
                WHEN b.position IN ('WR', 'TE', 'RB') AND b.offense_snaps > 0.0 AND b.eff_dropbacks > 0.0
                THEN LEAST({ub_sql}, GREATEST(b.targets, b.eff_dropbacks * {frac_sql}))
                ELSE b.targets
            END
        AS DOUBLE)
    """

    extra_ctes = [c for c in (team_db_cte, snap_cte, opp_cte, ngs_cte) if c.strip()]
    ctes_prefix = ",\n".join(extra_ctes) + "," if extra_ctes else ""

    sql = f"""
        CREATE OR REPLACE TABLE main.player_week_adv AS
        WITH {ctes_prefix}
        team_psw AS (
            SELECT
                CAST(s.season AS INTEGER) AS season,
                CAST(s.week AS INTEGER) AS week,
                {tm_expr} AS team,
                CAST(SUM({car_expr}) AS DOUBLE) AS team_carries,
                CAST(SUM({att_expr} + {sacks_expr}) AS DOUBLE) AS psw_dropbacks
            FROM nfl.player_stats_week s
            WHERE UPPER(TRIM(CAST(s.position AS VARCHAR))) IN ('QB', 'RB', 'WR', 'TE')
              AND s.player_id IS NOT NULL
              AND TRIM(CAST(s.player_id AS VARCHAR)) != ''
              AND TRY_CAST(s.week AS INTEGER) >= 1
              {psw_st_filter}
            GROUP BY 1, 2, 3
        ),
        base AS (
            SELECT
                CAST(s.player_id AS VARCHAR) AS gsis_id,
                {name_expr} AS player_name,
                UPPER(TRIM(CAST(s.position AS VARCHAR))) AS position,
                {tm_expr} AS team,
                {opp_tm_expr} AS opponent_team,
                CAST(s.season AS INTEGER) AS season,
                CAST(s.week AS INTEGER) AS week,
                {team_db_val} AS team_dropbacks,
                {eff_db_val} AS eff_dropbacks,
                CAST(COALESCE(tc.team_carries, 25.0) AS DOUBLE) AS team_carries,
                {off_snaps_expr} AS offense_snaps,
                {snap_pct_expr} AS snap_pct,
                {att_expr} AS attempts,
                {sacks_expr} AS sacks_suffered,
                CAST(({att_expr} + {sacks_expr}) AS DOUBLE) AS pass_dropbacks,
                {car_expr} AS carries,
                {tgt_expr} AS targets,
                {rec_expr} AS receptions,
                {rec_yds_expr} AS receiving_yards,
                {pass_yds_expr} AS passing_yards,
                {pass_air_expr} AS passing_air_yards,
                {rec_air_expr} AS receiving_air_yards,
                {tgt_sh_expr} AS target_share,
                {air_sh_expr} AS air_yards_share,
                {wopr_expr} AS wopr,
                CAST(({team_db_val} * {snap_pct_expr}) AS DOUBLE) AS est_routes,
                {ngs_att_expr} AS ngs_rush_attempts,
                {ryoe_expr} AS ryoe,
                {epa_expr} AS passing_epa,
                {cpoe_expr} AS passing_cpoe,
                {ff_xfp_expr} AS ff_xfp_raw
            FROM nfl.player_stats_week s
            LEFT JOIN team_psw tc
              ON tc.season = CAST(s.season AS INTEGER)
             AND tc.week = CAST(s.week AS INTEGER)
             AND tc.team = {tm_expr}
            LEFT JOIN team_db db
              ON db.season = CAST(s.season AS INTEGER)
             AND db.week = CAST(s.week AS INTEGER)
             AND db.team = {tm_expr}
            {snap_join}
            {opp_join}
            {ngs_join}
            WHERE UPPER(TRIM(CAST(s.position AS VARCHAR))) IN ('QB', 'RB', 'WR', 'TE')
              AND s.player_id IS NOT NULL
              AND TRIM(CAST(s.player_id AS VARCHAR)) != ''
              AND TRY_CAST(s.week AS INTEGER) >= 1
              {psw_st_filter}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(s.player_id AS VARCHAR), CAST(s.season AS INTEGER), CAST(s.week AS INTEGER)
                ORDER BY ({att_expr} + {car_expr} + {tgt_expr}) DESC
            ) = 1
        )
        SELECT
            b.gsis_id,
            b.player_name,
            b.position,
            b.team,
            b.opponent_team,
            b.season,
            b.week,
            b.team_dropbacks,
            b.eff_dropbacks,
            b.team_carries,
            b.offense_snaps,
            b.snap_pct,
            b.attempts,
            b.sacks_suffered,
            b.pass_dropbacks,
            b.carries,
            b.targets,
            b.receptions,
            b.receiving_yards,
            b.passing_yards,
            b.passing_air_yards,
            b.receiving_air_yards,
            b.target_share,
            b.air_yards_share,
            b.wopr,
            b.est_routes,
            ({constrained_routes_sql}) AS constrained_routes,
            CAST(COALESCE(b.receiving_yards / NULLIF(b.est_routes, 0.0), 0.0) AS DOUBLE) AS yprr,
            CAST(COALESCE(b.receiving_yards / NULLIF(({constrained_routes_sql}), 0.0), 0.0) AS DOUBLE) AS yprr_constrained,
            CAST(COALESCE(b.targets / NULLIF(b.est_routes, 0.0) * 100.0, 0.0) AS DOUBLE) AS tprr_pct,
            CAST(COALESCE(b.receiving_air_yards / NULLIF(b.targets, 0.0), 0.0) AS DOUBLE) AS rec_adot,
            b.ngs_rush_attempts,
            b.ryoe,
            CAST(COALESCE(b.ryoe / NULLIF(b.ngs_rush_attempts, 0.0), 0.0) AS DOUBLE) AS ryoe_per_att,
            b.passing_epa,
            CAST(COALESCE(b.passing_epa / NULLIF(b.pass_dropbacks, 0.0), 0.0) AS DOUBLE) AS epa_per_db,
            b.passing_cpoe,
            CAST(COALESCE(b.passing_air_yards / NULLIF(b.attempts, 0.0), 0.0) AS DOUBLE) AS pass_adot,
            b.ff_xfp_raw
        FROM base b
        ORDER BY b.season, b.week, b.gsis_id
    """
    con.execute(sql)
    cnt = int(con.execute("SELECT COUNT(*) FROM main.player_week_adv").fetchone()[0])
    log(f"Loaded {cnt} rows into main.player_week_adv")
    return cnt


def _actual_expr(*, rec_weight: float, int_weight: float) -> str:
    return (
        f"CAST((0.04 * b.passing_yards + 4.0 * b.passing_tds + ({int_weight}) * b.passing_interceptions "
        f"+ 0.1 * b.rushing_yards + 6.0 * b.rushing_tds + 0.1 * b.receiving_yards "
        f"+ ({rec_weight}) * b.receptions + 6.0 * b.receiving_tds + 2.0 * b.two_pt_actual "
        f"- 2.0 * b.fumbles_lost) AS DOUBLE)"
    )


def _xfp_expr(
    *,
    rec_weight: float,
    int_weight: float,
    actual_col_sql: str,
) -> str:
    lam_fl = (
        f"(CASE b.position "
        f"WHEN 'QB' THEN {EMPIRICAL_FUMBLE_LOST_RATE['QB']} "
        f"WHEN 'RB' THEN {EMPIRICAL_FUMBLE_LOST_RATE['RB']} "
        f"WHEN 'WR' THEN {EMPIRICAL_FUMBLE_LOST_RATE['WR']} "
        f"WHEN 'TE' THEN {EMPIRICAL_FUMBLE_LOST_RATE['TE']} ELSE 0.005 END)"
    )
    touches = (
        "GREATEST(0.0, b.rush_attempts_exp + b.receptions_exp + "
        "CASE WHEN b.position = 'QB' THEN b.sacks_suffered ELSE 0.0 END)"
    )
    raw = (
        f"(0.04 * b.pass_yds_exp + 4.0 * b.pass_td_exp + ({int_weight}) * b.pass_int_exp "
        f"+ 0.1 * b.rush_yds_exp + 6.0 * b.rush_td_exp + 0.1 * b.rec_yds_exp "
        f"+ ({rec_weight}) * b.receptions_exp + 6.0 * b.rec_td_exp + 2.0 * b.two_pt_exp "
        f"- 2.0 * {lam_fl} * {touches})"
    )
    return (
        f"CAST(CASE WHEN b.has_ff_opp THEN GREATEST(0.0, {raw}) "
        f"ELSE GREATEST(0.0, {actual_col_sql}) END AS DOUBLE)"
    )


def build_player_week_xfp(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None] = print,
) -> int:
    """Materialize main.player_week_xfp with component expectations and standard scoring presets."""
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    if not _rel_exists(con, "nfl.player_stats_week"):
        _create_empty_xfp_table(con)
        log("nfl.player_stats_week missing; created empty main.player_week_xfp")
        return 0

    psw_cols = _table_cols(con, "nfl.player_stats_week")
    if not {"player_id", "position", "season", "week"}.issubset(psw_cols):
        _create_empty_xfp_table(con)
        log("nfl.player_stats_week missing required columns; created empty main.player_week_xfp")
        return 0

    opp_cols = _table_cols(con, "nfl.ff_opportunity")
    has_opp = {"player_id", "season", "week"}.issubset(opp_cols)

    psw_st_filter = (
        "AND UPPER(TRIM(CAST(s.season_type AS VARCHAR))) = 'REG'"
        if "season_type" in psw_cols
        else ""
    )

    tm_expr = _psw_team_expr(psw_cols, "s")
    opp_tm_expr = _sql_norm_team("s.opponent_team") if "opponent_team" in psw_cols else "CAST('' AS VARCHAR)"
    name_expr = _psw_name_expr(psw_cols, "s")
    sacks_expr = _psw_col(psw_cols, "sacks_suffered")

    fum_terms = [
        _psw_col(psw_cols, c)
        for c in ("rushing_fumbles_lost", "receiving_fumbles_lost", "sack_fumbles_lost")
        if c in psw_cols
    ]
    fum_expr = "CAST((" + " + ".join(fum_terms) + ") AS DOUBLE)" if fum_terms else "CAST(0.0 AS DOUBLE)"

    twopt_terms = [
        _psw_col(psw_cols, c)
        for c in ("passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions")
        if c in psw_cols
    ]
    twopt_expr = "CAST((" + " + ".join(twopt_terms) + ") AS DOUBLE)" if twopt_terms else "CAST(0.0 AS DOUBLE)"

    if has_opp:
        opp_st_filter = (
            "AND UPPER(TRIM(CAST(season_type AS VARCHAR))) = 'REG'"
            if "season_type" in opp_cols
            else ""
        )

        def _oc(c: str) -> str:
            return (
                f"COALESCE(CAST({c} AS DOUBLE), CAST(0.0 AS DOUBLE))"
                if c.lower() in opp_cols
                else "CAST(0.0 AS DOUBLE)"
            )

        opp_cte = f"""
        WITH opp_dedup AS (
            SELECT
                CAST(player_id AS VARCHAR) AS player_id,
                CAST(season AS INTEGER) AS season,
                CAST(week AS INTEGER) AS week,
                {_oc('pass_yards_gained_exp')} AS pass_yds_exp,
                {_oc('pass_touchdown_exp')} AS pass_td_exp,
                {_oc('pass_interception_exp')} AS pass_int_exp,
                {_oc('rush_yards_gained_exp')} AS rush_yds_exp,
                {_oc('rush_touchdown_exp')} AS rush_td_exp,
                {_oc('rush_attempt')} AS rush_attempts_exp,
                {_oc('rec_yards_gained_exp')} AS rec_yds_exp,
                {_oc('receptions_exp')} AS receptions_exp,
                {_oc('rec_touchdown_exp')} AS rec_td_exp,
                CAST(({_oc('pass_two_point_conv_exp')} + {_oc('rec_two_point_conv_exp')} + {_oc('rush_two_point_conv_exp')}) AS DOUBLE) AS two_pt_exp,
                {_oc('total_fantasy_points_exp')} AS ff_total_xfp
            FROM nfl.ff_opportunity
            WHERE player_id IS NOT NULL
              AND TRIM(CAST(player_id AS VARCHAR)) != ''
              AND TRY_CAST(week AS INTEGER) >= 1
              {opp_st_filter}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(player_id AS VARCHAR), CAST(season AS INTEGER), CAST(week AS INTEGER)
                ORDER BY {_oc('total_fantasy_points_exp')} DESC
            ) = 1
        ),
        """
        opp_join = """
        LEFT JOIN opp_dedup o
          ON o.player_id = CAST(s.player_id AS VARCHAR)
         AND o.season = CAST(s.season AS INTEGER)
         AND o.week = CAST(s.week AS INTEGER)
        """
        has_opp_expr = "CAST((o.player_id IS NOT NULL) AS BOOLEAN)"
        exp_cols_sql = """
            COALESCE(o.pass_yds_exp, CAST(0.0 AS DOUBLE)) AS pass_yds_exp,
            COALESCE(o.pass_td_exp, CAST(0.0 AS DOUBLE)) AS pass_td_exp,
            COALESCE(o.pass_int_exp, CAST(0.0 AS DOUBLE)) AS pass_int_exp,
            COALESCE(o.rush_yds_exp, CAST(0.0 AS DOUBLE)) AS rush_yds_exp,
            COALESCE(o.rush_td_exp, CAST(0.0 AS DOUBLE)) AS rush_td_exp,
            COALESCE(o.rush_attempts_exp, CAST(0.0 AS DOUBLE)) AS rush_attempts_exp,
            COALESCE(o.rec_yds_exp, CAST(0.0 AS DOUBLE)) AS rec_yds_exp,
            COALESCE(o.receptions_exp, CAST(0.0 AS DOUBLE)) AS receptions_exp,
            COALESCE(o.rec_td_exp, CAST(0.0 AS DOUBLE)) AS rec_td_exp,
            COALESCE(o.two_pt_exp, CAST(0.0 AS DOUBLE)) AS two_pt_exp,
            COALESCE(o.ff_total_xfp, CAST(0.0 AS DOUBLE)) AS ff_total_xfp,
        """
    else:
        opp_cte = "WITH "
        opp_join = ""
        has_opp_expr = "CAST(FALSE AS BOOLEAN)"
        exp_cols_sql = """
            CAST(0.0 AS DOUBLE) AS pass_yds_exp,
            CAST(0.0 AS DOUBLE) AS pass_td_exp,
            CAST(0.0 AS DOUBLE) AS pass_int_exp,
            CAST(0.0 AS DOUBLE) AS rush_yds_exp,
            CAST(0.0 AS DOUBLE) AS rush_td_exp,
            CAST(0.0 AS DOUBLE) AS rush_attempts_exp,
            CAST(0.0 AS DOUBLE) AS rec_yds_exp,
            CAST(0.0 AS DOUBLE) AS receptions_exp,
            CAST(0.0 AS DOUBLE) AS rec_td_exp,
            CAST(0.0 AS DOUBLE) AS two_pt_exp,
            CAST(0.0 AS DOUBLE) AS ff_total_xfp,
        """

    act_ppr = _actual_expr(rec_weight=1.0, int_weight=-2.0)
    act_half = _actual_expr(rec_weight=0.5, int_weight=-2.0)
    act_std = _actual_expr(rec_weight=0.0, int_weight=-2.0)
    act_yahoo = _actual_expr(rec_weight=0.5, int_weight=-1.0)

    xfp_ppr = _xfp_expr(rec_weight=1.0, int_weight=-2.0, actual_col_sql=act_ppr)
    xfp_half = _xfp_expr(rec_weight=0.5, int_weight=-2.0, actual_col_sql=act_half)
    xfp_std = _xfp_expr(rec_weight=0.0, int_weight=-2.0, actual_col_sql=act_std)
    xfp_yahoo = _xfp_expr(rec_weight=0.5, int_weight=-1.0, actual_col_sql=act_yahoo)

    sql = f"""
        CREATE OR REPLACE TABLE main.player_week_xfp AS
        {opp_cte}
        base AS (
            SELECT
                CAST(s.player_id AS VARCHAR) AS gsis_id,
                {name_expr} AS player_name,
                UPPER(TRIM(CAST(s.position AS VARCHAR))) AS position,
                {tm_expr} AS team,
                {opp_tm_expr} AS opponent_team,
                CAST(s.season AS INTEGER) AS season,
                CAST(s.week AS INTEGER) AS week,
                {has_opp_expr} AS has_ff_opp,
                {exp_cols_sql}
                {sacks_expr} AS sacks_suffered,
                {_psw_col(psw_cols, 'passing_yards')} AS passing_yards,
                {_psw_col(psw_cols, 'passing_tds')} AS passing_tds,
                {_psw_col(psw_cols, 'passing_interceptions')} AS passing_interceptions,
                {_psw_col(psw_cols, 'rushing_yards')} AS rushing_yards,
                {_psw_col(psw_cols, 'rushing_tds')} AS rushing_tds,
                {_psw_col(psw_cols, 'receptions')} AS receptions,
                {_psw_col(psw_cols, 'receiving_yards')} AS receiving_yards,
                {_psw_col(psw_cols, 'receiving_tds')} AS receiving_tds,
                {fum_expr} AS fumbles_lost,
                {twopt_expr} AS two_pt_actual
            FROM nfl.player_stats_week s
            {opp_join}
            WHERE UPPER(TRIM(CAST(s.position AS VARCHAR))) IN ('QB', 'RB', 'WR', 'TE')
              AND s.player_id IS NOT NULL
              AND TRIM(CAST(s.player_id AS VARCHAR)) != ''
              AND TRY_CAST(s.week AS INTEGER) >= 1
              {psw_st_filter}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(s.player_id AS VARCHAR), CAST(s.season AS INTEGER), CAST(s.week AS INTEGER)
                ORDER BY ({_psw_col(psw_cols, 'attempts')} + {_psw_col(psw_cols, 'carries')} + {_psw_col(psw_cols, 'targets')}) DESC
            ) = 1
        ),
        scored AS (
            SELECT
                b.gsis_id,
                b.player_name,
                b.position,
                b.team,
                b.opponent_team,
                b.season,
                b.week,
                b.has_ff_opp,
                b.pass_yds_exp,
                b.pass_td_exp,
                b.pass_int_exp,
                b.rush_yds_exp,
                b.rush_td_exp,
                b.rush_attempts_exp,
                b.rec_yds_exp,
                b.receptions_exp,
                b.rec_td_exp,
                b.two_pt_exp,
                b.sacks_suffered,
                b.passing_yards,
                b.passing_tds,
                b.passing_interceptions,
                b.rushing_yards,
                b.rushing_tds,
                b.receptions,
                b.receiving_yards,
                b.receiving_tds,
                b.fumbles_lost,
                b.two_pt_actual,
                ({xfp_ppr}) AS xfp_ppr,
                ({xfp_half}) AS xfp_half,
                ({xfp_std}) AS xfp_std,
                ({xfp_yahoo}) AS xfp_yahoo,
                ({act_ppr}) AS actual_ppr,
                ({act_half}) AS actual_half,
                ({act_std}) AS actual_std,
                ({act_yahoo}) AS actual_yahoo,
                b.ff_total_xfp
            FROM base b
        )
        SELECT
            gsis_id,
            player_name,
            position,
            team,
            opponent_team,
            season,
            week,
            has_ff_opp,
            pass_yds_exp,
            pass_td_exp,
            pass_int_exp,
            rush_yds_exp,
            rush_td_exp,
            rush_attempts_exp,
            rec_yds_exp,
            receptions_exp,
            rec_td_exp,
            two_pt_exp,
            sacks_suffered,
            passing_yards,
            passing_tds,
            passing_interceptions,
            rushing_yards,
            rushing_tds,
            receptions,
            receiving_yards,
            receiving_tds,
            fumbles_lost,
            two_pt_actual,
            xfp_ppr,
            xfp_half,
            xfp_std,
            xfp_yahoo,
            actual_ppr,
            actual_half,
            actual_std,
            actual_yahoo,
            CAST((actual_ppr - xfp_ppr) AS DOUBLE) AS luck_delta_ppr,
            CAST((actual_half - xfp_half) AS DOUBLE) AS luck_delta_half,
            CAST((actual_std - xfp_std) AS DOUBLE) AS luck_delta_std,
            CAST((actual_yahoo - xfp_yahoo) AS DOUBLE) AS luck_delta_yahoo,
            ff_total_xfp
        FROM scored
        ORDER BY season, week, gsis_id
    """
    con.execute(sql)
    cnt = int(con.execute("SELECT COUNT(*) FROM main.player_week_xfp").fetchone()[0])
    log(f"Loaded {cnt} rows into main.player_week_xfp")
    return cnt


def build_team_week_dvp(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None] = print,
) -> int:
    """Materialize main.team_week_dvp from main.player_week_xfp by defense_team & position."""
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    if not _rel_exists(con, "main.player_week_xfp"):
        _create_empty_dvp_table(con)
        log("main.player_week_xfp missing; created empty main.team_week_dvp")
        return 0

    con.execute(
        """
        CREATE OR REPLACE TABLE main.team_week_dvp AS
        SELECT
            opponent_team AS defense_team,
            MAX(team) AS offense_team,
            position,
            season,
            week,
            CAST(COUNT(*) AS INTEGER) AS player_count,
            CAST(SUM(actual_ppr) AS DOUBLE) AS pts_allowed_ppr,
            CAST(SUM(actual_half) AS DOUBLE) AS pts_allowed_half,
            CAST(SUM(actual_std) AS DOUBLE) AS pts_allowed_std,
            CAST(SUM(actual_yahoo) AS DOUBLE) AS pts_allowed_yahoo,
            CAST(SUM(xfp_ppr) AS DOUBLE) AS xfp_allowed_ppr,
            CAST(SUM(xfp_half) AS DOUBLE) AS xfp_allowed_half,
            CAST(SUM(xfp_std) AS DOUBLE) AS xfp_allowed_std,
            CAST(SUM(xfp_yahoo) AS DOUBLE) AS xfp_allowed_yahoo
        FROM main.player_week_xfp
        WHERE opponent_team IS NOT NULL AND opponent_team != ''
        GROUP BY 1, 3, 4, 5
        ORDER BY season, week, defense_team, position
        """
    )
    cnt = int(con.execute("SELECT COUNT(*) FROM main.team_week_dvp").fetchone()[0])
    log(f"Loaded {cnt} rows into main.team_week_dvp")
    return cnt


def build_advanced_stats_tables(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None] = print,
) -> dict[str, int]:
    """Build all three league-agnostic advanced stats Gold tables in dependency order."""
    adv_cnt = build_player_week_adv(con, log=log)
    xfp_cnt = build_player_week_xfp(con, log=log)
    dvp_cnt = build_team_week_dvp(con, log=log)
    return {
        "player_week_adv": adv_cnt,
        "player_week_xfp": xfp_cnt,
        "team_week_dvp": dvp_cnt,
    }
