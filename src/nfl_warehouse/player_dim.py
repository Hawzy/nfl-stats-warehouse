"""Build main.player_dim cross-platform player directory purely from public nflverse views."""

from dataclasses import dataclass
from typing import Callable

import duckdb

__all__ = ["PlayerDimResult", "build_player_dim"]

_EXTERNAL_IDS: tuple[str, ...] = (
    "mfl_id",
    "yahoo_id",
    "fantasypros_id",
    "sleeper_id",
    "espn_id",
    "pfr_id",
    "sportradar_id",
    "rotowire_id",
    "pff_id",
)


@dataclass(frozen=True)
class PlayerDimResult:
    """Summary of main.player_dim build results."""

    rows: int
    dropped_no_gsis: int


def _norm_id_sql(expr: str) -> str:
    """Return SQL normalizing an external identifier to clean VARCHAR or NULL."""
    return (
        f"CASE "
        f"WHEN {expr} IS NULL THEN NULL "
        f"WHEN lower(trim(CAST({expr} AS VARCHAR))) IN ('', 'na', 'nan', 'none', 'null') THEN NULL "
        f"ELSE regexp_replace(trim(CAST({expr} AS VARCHAR)), '^([0-9]+)\\.0+$', '\\1') "
        f"END"
    )


def _coalesce_sources(
    sources: list[tuple[str, tuple[str, ...], set[str]]],
    *,
    cast_as: str | None = None,
) -> str:
    """Build a coalesce expression across ordered (alias, candidates, available_cols) sources."""
    present: list[str] = []
    for alias, candidates, available in sources:
        for col in candidates:
            if col in available:
                raw_ref = f"{alias}.{col}"
                if cast_as:
                    present.append(f"TRY_CAST({raw_ref} AS {cast_as})")
                else:
                    present.append(f"NULLIF(trim(CAST({raw_ref} AS VARCHAR)), '')")
    if not present:
        return f"CAST(NULL AS {cast_as or 'VARCHAR'})"
    return present[0] if len(present) == 1 else f"coalesce({', '.join(present)})"


def _resolve_ext_id_expr(
    id_name: str,
    cols_players: set[str],
    cols_ff: set[str],
    *,
    prefer_players: bool = False,
) -> str:
    """Build normalized SQL expression resolving an external ID across players & ff_playerids."""
    ff_expr = _norm_id_sql(f"ff.{id_name}") if id_name in cols_ff else None
    p_expr = _norm_id_sql(f"p.{id_name}") if id_name in cols_players else None
    ordered = (p_expr, ff_expr) if prefer_players else (ff_expr, p_expr)
    available = [expr for expr in ordered if expr is not None]
    if not available:
        return "CAST(NULL AS VARCHAR)"
    return available[0] if len(available) == 1 else f"coalesce({available[0]}, {available[1]})"


def _ff_order_by_sql(cols_ff: set[str]) -> str:
    """Return deterministic tie-breaking ORDER BY clause for ff_playerids deduplication."""
    parts: list[str] = []
    if "db_season" in cols_ff:
        parts.append("TRY_CAST(db_season AS INT) DESC NULLS LAST")

    present_ids = [col for col in _EXTERNAL_IDS if col in cols_ff]
    if present_ids:
        score_sum = " + ".join(
            f"(CASE WHEN {_norm_id_sql(col)} IS NOT NULL THEN 1 ELSE 0 END)"
            for col in present_ids
        )
        parts.append(f"({score_sum}) DESC")

    for col in ("mfl_id", "yahoo_id", "fantasypros_id", "sleeper_id"):
        if col in cols_ff:
            parts.append(f"{col} DESC NULLS LAST")

    return ", ".join(parts) if parts else "1"


def build_player_dim(
    con: duckdb.DuckDBPyConnection,
    log: Callable[[str], None] = print,
) -> PlayerDimResult:
    """Create or replace main.player_dim from nfl.players and nfl.ff_playerids."""
    try:
        desc_players = con.execute("DESCRIBE nfl.players").fetchall()
    except duckdb.Error:
        log("nfl.players view does not exist; skipping player_dim build")
        return PlayerDimResult(0, 0)

    try:
        desc_ff = con.execute("DESCRIBE nfl.ff_playerids").fetchall()
    except duckdb.Error:
        log("nfl.ff_playerids view does not exist; skipping player_dim build")
        return PlayerDimResult(0, 0)

    cols_players = {row[0].lower() for row in desc_players}
    cols_ff = {row[0].lower() for row in desc_ff}

    if "gsis_id" in cols_ff:
        dropped_no_gsis = con.execute(
            "SELECT count(*) FROM nfl.ff_playerids "
            "WHERE gsis_id IS NULL OR trim(CAST(gsis_id AS VARCHAR)) = ''"
        ).fetchone()[0]
    else:
        dropped_no_gsis = con.execute("SELECT count(*) FROM nfl.ff_playerids").fetchone()[0]

    display_name_expr = _coalesce_sources(
        [("p", ("display_name", "name"), cols_players), ("ff", ("name",), cols_ff)]
    )
    position_expr = _coalesce_sources(
        [("p", ("position",), cols_players), ("ff", ("position",), cols_ff)]
    )
    birth_date_expr = _coalesce_sources(
        [
            ("p", ("birth_date", "birthdate"), cols_players),
            ("ff", ("birthdate", "birth_date"), cols_ff),
        ],
        cast_as="DATE",
    )
    height_expr = _coalesce_sources(
        [("p", ("height",), cols_players), ("ff", ("height",), cols_ff)],
        cast_as="DOUBLE",
    )
    weight_expr = _coalesce_sources(
        [("p", ("weight",), cols_players), ("ff", ("weight",), cols_ff)],
        cast_as="DOUBLE",
    )
    college_expr = _coalesce_sources(
        [
            ("p", ("college_name", "college"), cols_players),
            ("ff", ("college", "college_name"), cols_ff),
        ]
    )
    draft_year_expr = _coalesce_sources(
        [("p", ("draft_year",), cols_players), ("ff", ("draft_year",), cols_ff)],
        cast_as="INT",
    )
    draft_round_expr = _coalesce_sources(
        [("p", ("draft_round",), cols_players), ("ff", ("draft_round",), cols_ff)],
        cast_as="INT",
    )
    draft_pick_expr = _coalesce_sources(
        [
            ("p", ("draft_pick", "draft_number"), cols_players),
            ("ff", ("draft_pick", "draft_ovr"), cols_ff),
        ],
        cast_as="INT",
    )

    fallback_merge_name = (
        f"trim(regexp_replace(regexp_replace(lower({display_name_expr}), '[^a-z ]', '', 'g'), "
        f"'\\s+(jr|sr|ii|iii|iv|v)$', ''))"
    )
    if "merge_name" in cols_ff:
        merge_name_expr = (
            f"coalesce(NULLIF(trim(CAST(ff.merge_name AS VARCHAR)), ''), {fallback_merge_name})"
        )
    else:
        merge_name_expr = fallback_merge_name

    ext_selects = [
        f"{_resolve_ext_id_expr(id_name, cols_players, cols_ff, prefer_players=(id_name == 'pfr_id'))} AS {id_name}"
        for id_name in _EXTERNAL_IDS
    ]

    p_order = (
        "TRY_CAST(last_season AS INT) DESC NULLS LAST, gsis_id"
        if "last_season" in cols_players
        else "gsis_id"
    )
    ff_order = _ff_order_by_sql(cols_ff)

    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        """
        CREATE OR REPLACE TABLE main.player_dim (
            gsis_id VARCHAR PRIMARY KEY,
            display_name VARCHAR,
            merge_name VARCHAR,
            position VARCHAR,
            birth_date DATE,
            height DOUBLE,
            weight DOUBLE,
            college VARCHAR,
            draft_year INT,
            draft_round INT,
            draft_pick INT,
            mfl_id VARCHAR,
            yahoo_id VARCHAR,
            fantasypros_id VARCHAR,
            sleeper_id VARCHAR,
            espn_id VARCHAR,
            pfr_id VARCHAR,
            sportradar_id VARCHAR,
            rotowire_id VARCHAR,
            pff_id VARCHAR
        )
        """
    )

    ext_col_names = ", ".join(_EXTERNAL_IDS)
    ext_col_exprs = ",\n        ".join(ext_selects)
    insert_sql = f"""
    WITH p_dedup AS (
        SELECT *
        FROM nfl.players
        WHERE gsis_id IS NOT NULL AND trim(CAST(gsis_id AS VARCHAR)) != ''
        QUALIFY row_number() OVER (PARTITION BY trim(CAST(gsis_id AS VARCHAR)) ORDER BY {p_order}) = 1
    ),
    ff_dedup AS (
        SELECT *
        FROM nfl.ff_playerids
        WHERE gsis_id IS NOT NULL AND trim(CAST(gsis_id AS VARCHAR)) != ''
        QUALIFY row_number() OVER (PARTITION BY trim(CAST(gsis_id AS VARCHAR)) ORDER BY {ff_order}) = 1
    )
    INSERT INTO main.player_dim (
        gsis_id, display_name, merge_name, position, birth_date,
        height, weight, college, draft_year, draft_round, draft_pick,
        {ext_col_names}
    )
    SELECT
        coalesce(trim(CAST(p.gsis_id AS VARCHAR)), trim(CAST(ff.gsis_id AS VARCHAR))) AS gsis_id,
        {display_name_expr} AS display_name,
        {merge_name_expr} AS merge_name,
        {position_expr} AS position,
        {birth_date_expr} AS birth_date,
        {height_expr} AS height,
        {weight_expr} AS weight,
        {college_expr} AS college,
        {draft_year_expr} AS draft_year,
        {draft_round_expr} AS draft_round,
        {draft_pick_expr} AS draft_pick,
        {ext_col_exprs}
    FROM p_dedup p
    FULL OUTER JOIN ff_dedup ff
      ON trim(CAST(p.gsis_id AS VARCHAR)) = trim(CAST(ff.gsis_id AS VARCHAR))
    """
    con.execute(insert_sql)

    rows = con.execute("SELECT count(*) FROM main.player_dim").fetchone()[0]
    log(
        f"player_dim: {rows:,} players "
        f"({dropped_no_gsis:,} ff_playerids rows without gsis_id dropped)"
    )
    return PlayerDimResult(rows=rows, dropped_no_gsis=dropped_no_gsis)
