"""Scoring config validation, main.site_scoring, player_week_points, and team_defense_week_points."""

import json
from pathlib import Path
import re
from typing import Any, Callable

import duckdb

from nfl_warehouse.mart import canon_team_sql

__all__ = [
    "load_scoring_config",
    "build_site_scoring",
    "build_player_week_points",
    "build_team_defense_week_points",
]

OFFENSIVE_POSITIONS: tuple[str, ...] = ("QB", "RB", "WR", "TE", "FB")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_POS_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_column_name(col: object, stat: str) -> str:
    """Ensure a stat column identifier is a safe unquoted SQL identifier."""
    if not isinstance(col, str) or not _IDENT_RE.fullmatch(col):
        raise ValueError(f"Invalid SQL column identifier {col!r} for stat '{stat}'")
    return col


def _validate_position_name(pos: object, context: str) -> str:
    """Ensure a position filter token is alphanumeric/underscore only."""
    if not isinstance(pos, str) or not _POS_RE.fullmatch(pos):
        raise ValueError(f"Invalid position identifier {pos!r} in {context}")
    return pos


def _validate_stat_spec(stat: object, spec: object) -> None:
    """Validate one stat_columns mapping entry."""
    if not isinstance(stat, str) or not stat:
        raise ValueError(f"Invalid stat name {stat!r} in 'stat_columns'")
    if isinstance(spec, list):
        for col in spec:
            _validate_column_name(col, stat)
        return
    if isinstance(spec, dict):
        if "prefer" in spec and "fallback" in spec:
            if not isinstance(spec["prefer"], list) or not isinstance(spec["fallback"], list):
                raise ValueError(f"Invalid prefer/fallback spec for stat '{stat}'")
            for col in spec["prefer"]:
                _validate_column_name(col, stat)
            for col in spec["fallback"]:
                _validate_column_name(col, stat)
            return
        if "columns" in spec and ("position" in spec or "positions" in spec):
            cols = spec["columns"]
            positions = spec.get("positions") or ([spec["position"]] if "position" in spec else [])
            if not isinstance(cols, list) or not isinstance(positions, list) or not positions:
                raise ValueError(f"Invalid position-gated spec for stat '{stat}'")
            for col in cols:
                _validate_column_name(col, stat)
            for pos in positions:
                _validate_position_name(pos, f"stat '{stat}'")
            return
    raise ValueError(f"Invalid column spec for stat '{stat}' in 'stat_columns'")


def _validate_ruleset_stats(
    rs: dict[str, Any],
    known_stats: set[str],
    known_dst_stats: set[str],
) -> None:
    """Validate rules, bonuses, position_rules, kicker_rules, and dst_rules for one ruleset."""
    site = rs.get("site")
    for stat in rs["rules"].keys():
        if stat not in known_stats:
            raise ValueError(f"unknown stat '{stat}' in ruleset for site '{site}'")

    for f_stat in rs.get("floor_zero_stats") or []:
        if f_stat not in known_stats:
            raise ValueError(f"unknown stat '{f_stat}' in floor_zero_stats for site '{site}'")

    bonuses = rs.get("bonuses")
    if bonuses is not None:
        if not isinstance(bonuses, list):
            raise ValueError(f"Ruleset '{site}' 'bonuses' must be a list")
        for b_idx, bonus in enumerate(bonuses):
            if not isinstance(bonus, dict):
                raise ValueError(f"Bonus {b_idx} in ruleset '{site}' must be a dict")
            for req in ("stat", "threshold", "points"):
                if req not in bonus:
                    raise ValueError(f"Bonus {b_idx} missing required key '{req}'")
            if bonus["stat"] not in known_stats:
                raise ValueError(f"unknown stat '{bonus['stat']}' in bonus for site '{site}'")

    pos_rules = rs.get("position_rules")
    if pos_rules is not None:
        if not isinstance(pos_rules, dict):
            raise ValueError(f"Ruleset '{site}' 'position_rules' must be a dict")
        for pos, p_map in pos_rules.items():
            _validate_position_name(pos, f"position_rules in '{site}'")
            if not isinstance(p_map, dict):
                raise ValueError(f"Invalid position_rules entry for '{pos}' in '{site}'")
            for stat in p_map.keys():
                if stat not in known_stats:
                    raise ValueError(f"unknown stat '{stat}' in position_rules for site '{site}'")

    kicker_rules = rs.get("kicker_rules")
    if kicker_rules is not None:
        if not isinstance(kicker_rules, dict):
            raise ValueError(f"Ruleset '{site}' 'kicker_rules' must be a dict")
        for stat in kicker_rules.keys():
            if stat not in known_stats:
                raise ValueError(f"unknown stat '{stat}' in kicker_rules for site '{site}'")

    dst_rules = rs.get("dst_rules")
    if dst_rules is not None:
        if not isinstance(dst_rules, dict):
            raise ValueError(f"Ruleset '{site}' 'dst_rules' must be a dict")
        for stat, val in dst_rules.items():
            if stat == "points_allowed_tiers":
                if not isinstance(val, list):
                    raise ValueError(f"'points_allowed_tiers' must be a list in '{site}'")
                for tier in val:
                    if not isinstance(tier, dict) or "points" not in tier or (
                        "min_pa" not in tier and "max_pa" not in tier
                    ):
                        raise ValueError(f"Invalid points_allowed tier in '{site}': {tier!r}")
                continue
            if stat not in known_dst_stats:
                raise ValueError(f"unknown stat '{stat}' in dst_rules for site '{site}'")


def load_scoring_config(path: Path | str) -> dict[str, Any]:
    """Load and validate scoring configuration from a JSON file."""
    with open(Path(path), "r", encoding="utf-8") as handle:
        config = json.load(handle)

    if not isinstance(config, dict):
        raise ValueError("Scoring config must be a JSON object")

    stat_columns = config.get("stat_columns")
    if not isinstance(stat_columns, dict):
        raise ValueError("Config missing or invalid 'stat_columns' dict")
    for stat, spec in stat_columns.items():
        _validate_stat_spec(stat, spec)

    dst_stat_columns = config.get("dst_stat_columns") or {}
    if not isinstance(dst_stat_columns, dict):
        raise ValueError("Config invalid 'dst_stat_columns' dict")
    for stat, spec in dst_stat_columns.items():
        _validate_stat_spec(stat, spec)

    rulesets = config.get("rulesets")
    if not isinstance(rulesets, list):
        raise ValueError("Config missing or invalid 'rulesets' list")

    known_stats = set(stat_columns.keys())
    known_dst_stats = set(dst_stat_columns.keys())
    required_keys = {"site", "valid_from_season", "source_url", "rules"}

    for idx, rs in enumerate(rulesets):
        if not isinstance(rs, dict):
            raise ValueError(f"Ruleset {idx} must be a dictionary")
        missing = required_keys - set(rs.keys())
        if missing:
            raise ValueError(f"Ruleset {idx} missing required keys: {sorted(missing)}")
        if not isinstance(rs["rules"], dict):
            raise ValueError(f"Ruleset {idx} 'rules' must be a dict")
        _validate_ruleset_stats(rs, known_stats, known_dst_stats)

    return config


def build_site_scoring(con: duckdb.DuckDBPyConnection, config: dict[str, Any]) -> int:
    """Create or replace main.site_scoring table and return inserted row count."""
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        """
        CREATE OR REPLACE TABLE main.site_scoring (
            site VARCHAR,
            valid_from_season INT,
            valid_to_season INT,
            scope VARCHAR,
            stat VARCHAR,
            kind VARCHAR,
            threshold DOUBLE,
            points DOUBLE
        )
        """
    )

    rows: list[tuple[Any, ...]] = []
    for ruleset in config.get("rulesets", []):
        site = str(ruleset["site"])
        valid_from = int(ruleset["valid_from_season"])
        valid_to_val = ruleset.get("valid_to_season")
        valid_to = int(valid_to_val) if valid_to_val is not None else None

        for stat, pts in ruleset.get("rules", {}).items():
            rows.append((site, valid_from, valid_to, "offense", str(stat), "per_unit", 0.0, float(pts)))

        for pos, p_map in (ruleset.get("position_rules") or {}).items():
            for stat, pts in p_map.items():
                rows.append((site, valid_from, valid_to, "offense", f"{pos}.{stat}", "per_unit", 0.0, float(pts)))

        for bonus in ruleset.get("bonuses") or []:
            rows.append(
                (
                    site,
                    valid_from,
                    valid_to,
                    "offense",
                    str(bonus["stat"]),
                    "bonus",
                    float(bonus["threshold"]),
                    float(bonus["points"]),
                )
            )

        for stat, pts in (ruleset.get("kicker_rules") or {}).items():
            rows.append((site, valid_from, valid_to, "kicker", str(stat), "per_unit", 0.0, float(pts)))

        for stat, val in (ruleset.get("dst_rules") or {}).items():
            if stat == "points_allowed_tiers":
                for tier in val:
                    thresh = float(tier.get("max_pa", tier.get("min_pa", 0.0)))
                    rows.append((site, valid_from, valid_to, "dst", "points_allowed", "tier", thresh, float(tier["points"])))
            else:
                rows.append((site, valid_from, valid_to, "dst", str(stat), "per_unit", 0.0, float(val)))

    if rows:
        con.executemany(
            """
            INSERT INTO main.site_scoring (
                site, valid_from_season, valid_to_season, scope, stat, kind, threshold, points
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def _sum_cols_expr(cols: list[str], existing_cols: set[str]) -> str | None:
    """Return SQL sum over present allowlisted columns, or None if none exist."""
    present = [
        c.lower()
        for c in cols
        if isinstance(c, str) and _IDENT_RE.fullmatch(c) and c.lower() in existing_cols
    ]
    if not present:
        return None
    return " + ".join(f"coalesce(CAST({c} AS DOUBLE), 0.0)" for c in present)


def _build_stat_expr_map(
    stat_columns: dict[str, Any],
    existing_cols: set[str],
) -> dict[str, str]:
    """Compile stat_columns definitions into DuckDB SQL expressions."""
    expr_map: dict[str, str] = {}
    for stat, spec in stat_columns.items():
        if isinstance(spec, list):
            expr_map[stat] = _sum_cols_expr(spec, existing_cols) or "0.0"
        elif "prefer" in spec:
            prefer_cols = [
                c.lower()
                for c in spec["prefer"]
                if isinstance(c, str) and _IDENT_RE.fullmatch(c) and c.lower() in existing_cols
            ]
            fallback_expr = _sum_cols_expr(spec["fallback"], existing_cols) or "0.0"
            if prefer_cols:
                prefer_expr = (
                    f"CAST({prefer_cols[0]} AS DOUBLE)"
                    if len(prefer_cols) == 1
                    else " + ".join(f"CAST({c} AS DOUBLE)" for c in prefer_cols)
                )
                expr_map[stat] = f"coalesce({prefer_expr}, ({fallback_expr}))"
            else:
                expr_map[stat] = fallback_expr
        else:
            positions = spec.get("positions") or [spec["position"]]
            valid_positions = [
                _validate_position_name(p, f"stat '{stat}'").upper().replace("'", "''")
                for p in positions
            ]
            pos_sql = ", ".join(f"'{p}'" for p in valid_positions)
            base_expr = _sum_cols_expr(spec["columns"], existing_cols)
            if base_expr is None:
                expr_map[stat] = "0.0"
            else:
                expr_map[stat] = (
                    f"(CASE WHEN upper(CAST(position AS VARCHAR)) IN ({pos_sql}) "
                    f"THEN ({base_expr}) ELSE 0.0 END)"
                )
    return expr_map


def _linear_terms(
    rules_map: dict[str, float],
    stat_expr_map: dict[str, str],
    *,
    floor_zero_stats: set[str] | None = None,
) -> list[str]:
    """Build weighted linear terms for a rules dictionary."""
    floor_set = floor_zero_stats or set()
    terms: list[str] = []
    for stat, mult in rules_map.items():
        mult_val = float(mult)
        if mult_val == 0.0:
            continue
        expr = stat_expr_map.get(stat, "0.0")
        if stat in floor_set:
            expr = f"greatest(({expr}), 0.0)"
        terms.append(f"({expr}) * {mult_val}")
    return terms


def _offense_formula(rs: dict[str, Any], stat_expr_map: dict[str, str]) -> str:
    """Compile offensive rules + position_rules + bonuses into a SQL formula."""
    floor_set = set(rs.get("floor_zero_stats") or [])
    terms = _linear_terms(rs.get("rules", {}), stat_expr_map, floor_zero_stats=floor_set)

    for pos, p_map in (rs.get("position_rules") or {}).items():
        _validate_position_name(pos, "position_rules")
        pos_escaped = str(pos).upper().replace("'", "''")
        for stat, mult in p_map.items():
            mult_val = float(mult)
            if mult_val == 0.0:
                continue
            expr = stat_expr_map.get(stat, "0.0")
            terms.append(
                f"(CASE WHEN upper(CAST(position AS VARCHAR)) = '{pos_escaped}' "
                f"THEN ({expr}) * {mult_val} ELSE 0.0 END)"
            )

    for bonus in rs.get("bonuses") or []:
        expr = stat_expr_map.get(bonus["stat"], "0.0")
        thresh = float(bonus["threshold"])
        b_pts = float(bonus["points"])
        terms.append(f"(CASE WHEN ({expr}) >= {thresh} THEN {b_pts} ELSE 0.0 END)")

    return " + ".join(terms) if terms else "0.0"


def build_player_week_points(
    con: duckdb.DuckDBPyConnection,
    config: dict[str, Any],
    log: Callable[[str], None] = print,
) -> int:
    """Create main.player_week_points for offense (and K when kicker_rules exist)."""
    try:
        cols_desc = con.execute("DESCRIBE nfl.player_stats_week").fetchall()
    except duckdb.Error:
        log("nfl.player_stats_week does not exist; skipping player_week_points build")
        return 0

    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    existing_cols = {row[0].lower() for row in cols_desc}

    if "player_display_name" in existing_cols and "player_name" in existing_cols:
        name_expr = "coalesce(player_display_name, player_name)"
    elif "player_display_name" in existing_cols:
        name_expr = "player_display_name"
    elif "player_name" in existing_cols:
        name_expr = "player_name"
    else:
        name_expr = "CAST(NULL AS VARCHAR)"

    if "team" in existing_cols and "recent_team" in existing_cols:
        team_expr = "coalesce(team, recent_team)"
    elif "team" in existing_cols:
        team_expr = "team"
    elif "recent_team" in existing_cols:
        team_expr = "recent_team"
    else:
        team_expr = "CAST(NULL AS VARCHAR)"

    stat_expr_map = _build_stat_expr_map(config.get("stat_columns", {}), existing_cols)
    rulesets = config.get("rulesets", [])
    if not rulesets:
        con.execute(
            """
            CREATE OR REPLACE TABLE main.player_week_points (
                player_id VARCHAR,
                player_name VARCHAR,
                position VARCHAR,
                team VARCHAR,
                season INT,
                week INT,
                season_type VARCHAR,
                site VARCHAR,
                points DOUBLE
            )
            """
        )
        return 0

    pos_list_sql = ", ".join(f"'{p}'" for p in OFFENSIVE_POSITIONS)
    subqueries: list[str] = []

    for rs in rulesets:
        site = str(rs["site"]).replace("'", "''")
        valid_from = int(rs["valid_from_season"])
        valid_to_val = rs.get("valid_to_season")
        valid_to = int(valid_to_val) if valid_to_val is not None else 9999

        off_formula = _offense_formula(rs, stat_expr_map)
        subqueries.append(
            f"""
            SELECT
                CAST(player_id AS VARCHAR) AS player_id,
                CAST({name_expr} AS VARCHAR) AS player_name,
                CAST(position AS VARCHAR) AS position,
                CAST({team_expr} AS VARCHAR) AS team,
                CAST(season AS INT) AS season,
                CAST(week AS INT) AS week,
                CAST(season_type AS VARCHAR) AS season_type,
                CAST('{site}' AS VARCHAR) AS site,
                ROUND(CAST(({off_formula}) AS DOUBLE), 4) AS points
            FROM nfl.player_stats_week
            WHERE position IN ({pos_list_sql})
              AND season BETWEEN {valid_from} AND {valid_to}
            """.strip()
        )

        kicker_rules = rs.get("kicker_rules") or {}
        if kicker_rules:
            k_terms = _linear_terms(kicker_rules, stat_expr_map)
            k_formula = " + ".join(k_terms) if k_terms else "0.0"
            subqueries.append(
                f"""
                SELECT
                    CAST(player_id AS VARCHAR) AS player_id,
                    CAST({name_expr} AS VARCHAR) AS player_name,
                    CAST(position AS VARCHAR) AS position,
                    CAST({team_expr} AS VARCHAR) AS team,
                    CAST(season AS INT) AS season,
                    CAST(week AS INT) AS week,
                    CAST(season_type AS VARCHAR) AS season_type,
                    CAST('{site}' AS VARCHAR) AS site,
                    ROUND(CAST(({k_formula}) AS DOUBLE), 4) AS points
                FROM nfl.player_stats_week
                WHERE position = 'K'
                  AND season BETWEEN {valid_from} AND {valid_to}
                """.strip()
            )

    full_sql = "CREATE OR REPLACE TABLE main.player_week_points AS " + "\nUNION ALL\n".join(subqueries)
    con.execute(full_sql)
    return con.execute("SELECT count(*) FROM main.player_week_points").fetchone()[0]


def _points_allowed_sql(tiers: list[dict[str, Any]]) -> str:
    """Compile points_allowed_tiers into a DuckDB CASE expression."""
    if not tiers:
        return "0.0"
    when_clauses: list[str] = []
    for tier in tiers:
        pts = float(tier["points"])
        min_pa = tier.get("min_pa")
        max_pa = tier.get("max_pa")
        if min_pa is not None and max_pa is not None:
            cond = f"s.points_allowed BETWEEN {int(min_pa)} AND {int(max_pa)}"
        elif max_pa is not None:
            cond = f"s.points_allowed <= {int(max_pa)}"
        else:
            cond = f"s.points_allowed >= {int(min_pa)}"
        when_clauses.append(f"WHEN {cond} THEN {pts}")
    return f"(CASE {' '.join(when_clauses)} ELSE 0.0 END)"


def build_team_defense_week_points(
    con: duckdb.DuckDBPyConnection,
    config: dict[str, Any],
    log: Callable[[str], None] = print,
) -> int:
    """Create main.team_defense_week_points from nfl.team_stats_week and nfl.schedules."""
    try:
        ts_desc = con.execute("DESCRIBE nfl.team_stats_week").fetchall()
    except duckdb.Error:
        log("nfl.team_stats_week does not exist; skipping team_defense_week_points build")
        return 0

    try:
        con.execute("DESCRIBE nfl.schedules").fetchall()
    except duckdb.Error:
        log("nfl.schedules does not exist; skipping team_defense_week_points build")
        return 0

    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    ts_cols = {row[0].lower() for row in ts_desc}
    dst_expr_map = _build_stat_expr_map(config.get("dst_stat_columns") or {}, ts_cols)
    dst_rulesets = [rs for rs in config.get("rulesets", []) if rs.get("dst_rules")]

    if not dst_rulesets:
        con.execute(
            """
            CREATE OR REPLACE TABLE main.team_defense_week_points (
                team VARCHAR,
                opponent_team VARCHAR,
                season INT,
                week INT,
                season_type VARCHAR,
                game_id VARCHAR,
                site VARCHAR,
                points_allowed INT,
                points DOUBLE
            )
            """
        )
        return 0

    t_canon = canon_team_sql("t.team")
    h_canon = canon_team_sql("home_team")
    a_canon = canon_team_sql("away_team")

    subqueries: list[str] = []
    for rs in dst_rulesets:
        site = str(rs["site"]).replace("'", "''")
        valid_from = int(rs["valid_from_season"])
        valid_to_val = rs.get("valid_to_season")
        valid_to = int(valid_to_val) if valid_to_val is not None else 9999

        dst_rules = dict(rs["dst_rules"])
        tiers = dst_rules.pop("points_allowed_tiers", [])
        stat_terms = _linear_terms(dst_rules, dst_expr_map)
        pa_term = _points_allowed_sql(tiers)
        terms = [pa_term, *stat_terms]
        formula = " + ".join(terms) if terms else "0.0"

        subqueries.append(
            f"""
            SELECT
                CAST({t_canon} AS VARCHAR) AS team,
                CAST({canon_team_sql('t.opponent_team')} AS VARCHAR) AS opponent_team,
                CAST(t.season AS INT) AS season,
                CAST(t.week AS INT) AS week,
                CAST(t.season_type AS VARCHAR) AS season_type,
                CAST(s.game_id AS VARCHAR) AS game_id,
                CAST('{site}' AS VARCHAR) AS site,
                CAST(s.points_allowed AS INT) AS points_allowed,
                ROUND(CAST(({formula}) AS DOUBLE), 4) AS points
            FROM nfl.team_stats_week t
            JOIN sched_dedup s
              ON s.season = CAST(t.season AS INT)
             AND s.week = CAST(t.week AS INT)
             AND s.canon_team = {t_canon}
            WHERE t.team IS NOT NULL
              AND s.points_allowed IS NOT NULL
              AND CAST(t.season AS INT) BETWEEN {valid_from} AND {valid_to}
            """.strip()
        )

    cte = f"""
    WITH sched_teams AS (
        SELECT
            CAST(season AS INT) AS season,
            CAST(week AS INT) AS week,
            CAST(game_id AS VARCHAR) AS game_id,
            {h_canon} AS canon_team,
            TRY_CAST(away_score AS INT) AS points_allowed
        FROM nfl.schedules
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
        UNION ALL
        SELECT
            CAST(season AS INT) AS season,
            CAST(week AS INT) AS week,
            CAST(game_id AS VARCHAR) AS game_id,
            {a_canon} AS canon_team,
            TRY_CAST(home_score AS INT) AS points_allowed
        FROM nfl.schedules
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
    ),
    sched_dedup AS (
        SELECT season, week, game_id, canon_team, points_allowed
        FROM sched_teams
        QUALIFY row_number() OVER (PARTITION BY season, week, canon_team ORDER BY game_id ASC) = 1
    )
    """
    full_sql = (
        "CREATE OR REPLACE TABLE main.team_defense_week_points AS\n"
        + cte
        + "\n"
        + "\nUNION ALL\n".join(subqueries)
    )
    con.execute(full_sql)
    return con.execute("SELECT count(*) FROM main.team_defense_week_points").fetchone()[0]
