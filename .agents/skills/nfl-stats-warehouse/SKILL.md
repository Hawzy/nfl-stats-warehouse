---
name: nfl-stats-warehouse
description: Use when hydrating, auditing, or querying the standalone public NFL stats DuckDB + nflverse Parquet warehouse (data/nfl_stats.duckdb), building point-in-time backtests with mart.* views, computing custom fantasy scoring, or attaching downstream SQLite/DuckDB databases read-only.
---

# NFL Stats Warehouse Reference (`nfl-stats-warehouse`)

## Overview

`nfl-stats-warehouse` is a standalone, zero-API-key analytical warehouse built over public **nflverse** Parquet datasets (`data/nflverse/`) and a portable DuckDB catalog (`data/nfl_stats.duckdb`).

All `nfl.*` views store relative Parquet paths resolved at runtime via `SET file_search_path`, so moving `data/` between Windows and Linux Cloud Remote Agents requires **no view rewrites**. Set `NFL_WAREHOUSE_DIR` (or pass `--data-dir`) to point at any external data directory.

## Quick Start: Opening in Python

Always use `nfl_warehouse.connect()` with `read_only=True` for analytical queries so concurrent readers never lock:

```python
from pathlib import Path
from nfl_warehouse import connect

with connect(read_only=True) as con:
    df = con.execute("""
        SELECT season, week, display_name, team, points
        FROM main.player_week_points p
        JOIN main.player_dim d ON d.gsis_id = p.player_id
        WHERE p.site = 'ppr' AND p.season = 2025 AND p.week = 1
        ORDER BY p.points DESC
        LIMIT 15
    """).pl()
```

Attach any private consumer SQLite or DuckDB databases strictly `READ_ONLY` at connection time:

```python
with connect(
    read_only=True,
    attach={"league": Path("path/to/league_history.db")},
) as con:
    rows = con.execute("SELECT * FROM league.player_performances LIMIT 5").fetchall()
```

## Schema Map (`data/nfl_stats.duckdb`)

### 1. Curated Public Tables (`main.*`)
- `main.player_dim`: 1-row-per-`gsis_id` crosswalk (`gsis_id`, `display_name`, `merge_name`, `position`, `birth_date`, `height`, `weight`, `college`, `draft_year`, `draft_round`, `draft_pick`, `mfl_id`, `yahoo_id`, `fantasypros_id`, `sleeper_id`, `espn_id`, `pfr_id`, `sportradar_id`, `rotowire_id`, `pff_id`). Intentionally omits `latest_team` to prevent point-in-time team leakage.
  - **UPSTREAM ID GAP**: Upstream `nflverse/ff_playerids` stopped populating `yahoo_id` starting with the **2025 draft class** (and recent UDFAs). For 2025+ rookies, prefer `gsis_id`, `mfl_id`, `sleeper_id`, `espn_id`, or `fantasypros_id` + normalized name matching.
- `main.site_scoring`: Flattened rules table (`site`, `valid_from_season`, `valid_to_season`, `scope`, `stat`, `kind`, `threshold`, `points`) loaded from `config/scoring.json` or `--scoring-config`.
- `main.player_week_points`: Weekly player fantasy points `(player_id, player_name, position, team, season, week, season_type, site, points)` for offense (`QB`, `RB`, `WR`, `TE`, `FB`) and Kickers (`K`), across universal presets (`ppr`, `half_ppr`, `standard`, `yahoo`) or custom configs (`te_rec`, `pass_fd`/`rush_fd`/`rec_fd` PPFD, bonuses).
  - Note on fumbles: `ppr` / `half_ppr` / `standard` use `fum_lost` (`rushing_fumbles_lost + receiving_fumbles_lost + sack_fumbles_lost`) for exact `0`-diff parity with `nfl.player_stats_week.fantasy_points_ppr`; `yahoo` uses `fum_lost_total` (`fumbles_lost_total`, capturing kick/punt-return and lateral fumbles lost).
- `main.team_defense_week_points`: Weekly Team Defense / Special Teams fantasy points `(team, opponent_team, season, week, season_type, game_id, site, points_allowed, points)` derived from `nfl.team_stats_week` + completed `nfl.schedules` scores.

### 2. Point-in-Time Backtesting Guardrail Layer (`mart.*`)
Use `mart.*` instead of raw `nfl.*` views whenever training or evaluating predictive / historical models:
- `mart.team_abbr_map`: Canonicalizes all current and legacy/PFR abbreviations (`LAR -> LA`, `JAC -> JAX`, `WSH -> WAS`, `STL/SL -> LA`, `SD -> LAC`, `OAK -> LV`, `ARZ -> ARI`, `BLT -> BAL`, `CLV -> CLE`, `HST -> HOU`).
- `mart.nextgen_passing_weekly`, `mart.nextgen_rushing_weekly`, `mart.nextgen_receiving_weekly`: Strips `week = 0` season-summary rows (`week >= 1`) and normalizes `team` (`LAR -> LA`).
- `mart.depth_charts_weekly`: Bridges pre-2025 weekly `season`/`week` rows (`legacy_weekly`) and 2025+ ISO-8601 `dt` snapshots (`dt_pregame`), selecting strictly the latest pre-kickoff snapshot (`dt_ts < kickoff_ts` in `America/New_York`, within 14 days) per `(season, week, team)`.
- `mart.player_team_week`: Deduplicated 1-row-per-`(season, week, gsis_id)` historical roster team from `nfl.rosters_weekly` with canonicalized `team`.

### 3. Raw `nflverse` Parquet Views (`nfl.*`)
All 25 cataloged views use `read_parquet(..., union_by_name = true)`:
- **Lite Profile (`--profile lite` / `--no-pbp`, ~66 MB Parquet / ~82 MB with DuckDB — recommended for Cloud Remote Agents & CI)**:
  - `nfl.player_stats_week`, `nfl.team_stats_week`, `nfl.schedules`, `nfl.rosters_weekly`, `nfl.snap_counts`, `nfl.injuries`, `nfl.depth_charts`, `nfl.nextgen_passing`, `nfl.nextgen_rushing`, `nfl.nextgen_receiving`, `nfl.ff_opportunity`, `nfl.players`, `nfl.ff_playerids`, `nfl.teams`, `nfl.trades`
- **Core Profile (`--profile core`, default, ~413 MB Parquet / ~428 MB with DuckDB)**: Lite + `nfl.pbp` (play-by-play, ~346 MB Parquet)
- **Full Profile (`--profile full` / `--include-optional`, ~445 MB Parquet / ~461 MB with DuckDB)**: Core + `nfl.participation`, `nfl.ftn_charting`, `nfl.pfr_adv_pass`, `nfl.pfr_adv_rush`, `nfl.pfr_adv_rec`, `nfl.pfr_adv_def`, `nfl.draft_picks`, `nfl.combine`, `nfl.contracts`

## Known Lookahead Traps & Schema Caveats
1. **`nfl.nextgen_*` `week = 0` rows:** Always use `mart.nextgen_*_weekly` or filter `week >= 1`.
2. **`nfl.participation` backfill & keys:** Published post-season only and has **no `season` or `week` columns** — key on `(nflverse_game_id, play_id)` joined to `nfl.pbp`/`nfl.schedules`; never use in predictive pre-game features.
3. **`nfl.depth_charts` 2025+ `dt` split:** `season` and `week` are `NULL` in 2025+; always use `mart.depth_charts_weekly` for unified weekly depth charts.
4. **`nfl.players` current metadata:** Never use `nfl.players.latest_team` or `nfl.players.status` for historical games; use `mart.player_team_week` (`nfl.rosters_weekly`).
5. **`nfl.ff_opportunity` null IDs:** Contains team-level unattributed plays with `player_id IS NULL`; filter `WHERE player_id IS NOT NULL` when joining to `main.player_dim`.
6. **DuckDB locking:** Close any open `connect(read_only=True)` sessions before running `--refresh` or `--rebuild`.

## Hydration, Rebuild & Integrity Audit CLI

```bash
# Fast hydration without heavy play-by-play (~66 MB Parquet / ~82 MB with DuckDB vs ~428 MB core)
python -m nfl_warehouse --profile lite

# Full core hydration or routine open-season refresh
python -m nfl_warehouse --refresh

# Rebuild DuckDB views, scoring (replaces default presets), player_dim, and mart.* offline from existing Parquet
python -m nfl_warehouse --rebuild --scoring-config config/scoring_te_premium_ppfd.example.json

# Recover from a corrupted _manifest.json using existing Parquet files on disk
python -m nfl_warehouse --repair-manifest

# Inspect manifest status without opening DuckDB
python -m nfl_warehouse --status

# Run automated data-integrity audit & emit hardware telemetry JSON
python scripts/audit_warehouse.py --profile lite --strict --telemetry-out docs/warehouse_telemetry.json
```
