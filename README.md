# NFL Stats Warehouse

A portable, zero-API-key **DuckDB + nflverse Parquet** NFL analytics warehouse designed for local research, reproducible point-in-time backtesting, and ephemeral **Google Cloud Remote Agents** / CI sandboxes.

## Features

- **100% Public & Free**: Hydrates 25 `nflverse` datasets (`16` core + `9` optional) via `nflreadpy` with zero API keys or private credentials.
- **Fast `--profile lite` (~66 MB Parquet / ~82 MB with DuckDB, vs ~428 MB core / ~461 MB full)**: Skips heavy play-by-play (`pbp`, ~346 MB Parquet) so cloud sandboxes and CI jobs hydrate the remaining core datasets in seconds. With `nflreadpy` `CacheMode.OFF`, Linux cold-hydration peak RSS is ~0.37–0.39 GiB (`lite`), ~0.60–0.80 GiB (`core`), and ~0.44–0.71 GiB (`full`) — all profiles fit inside 2–4 GiB Cloud Remote Agents.
- **Zero Hardcoded OS Paths**: `nfl.*` views store relative Parquet paths resolved via `SET GLOBAL file_search_path`, making `data/` portable across Windows, Linux containers, and `con.cursor()` without running `--rebuild`.
- **Point-in-Time Backtesting Guardrails (`mart.*`)**:
  - `mart.team_abbr_map` normalizes `LAR -> LA`, `JAC -> JAX`, `WSH -> WAS`, and historical/PFR codes (`STL`, `SL`, `SD`, `OAK`, `ARZ`, `BLT`, `CLV`, `HST`).
  - `mart.nextgen_{passing,rushing,receiving}_weekly` excludes `week = 0` season-summary rows.
  - `mart.depth_charts_weekly` unifies pre-2025 `season`/`week` files and 2025+ timestamped `dt` snapshots (adding canonical `unit` and strictly pre-kickoff `dt_ts < kickoff_ts` bounds in `America/New_York`); `mart.player_depth_rank_week` aggregates slots to 1 row per `(season, week, team, gsis_id)` for fanout-free joins.
  - `mart.player_team_week` provides historical weekly team membership from `rosters_weekly` instead of static `players.latest_team`. Warning: `team` is safe pre-kickoff for trades, while `status = 'INA'` reflects ~90-minute pre-kickoff gameday inactive announcements (avoid `status` in pre-gameday models).
- **Pluggable Scoring & Crosswalk (`main.*`)**:
  - `main.player_dim` joins `gsis_id` to `mfl_id`, `yahoo_id`, `sleeper_id`, `espn_id`, `fantasypros_id`, `pfr_id`, `sportradar_id`, `rotowire_id`, and `pff_id`.
  - `main.player_week_points` and `main.team_defense_week_points` compute `ppr`, `half_ppr`, `standard`, and `yahoo` presets (including two-way/non-offensive players with offensive or return stats, plus custom rules via `--scoring-config`, Kickers, Team DEF/DST, TE Premium, PPFD).
- **League-Agnostic Gold Analytical Tables (`main.*`)**:
  - `main.player_week_adv`: Deduplicated `REG`-season usage & efficiency (`snap_pct`, `est_routes`, `constrained_routes`, `yprr`, `yprr_constrained`, `tprr_pct`, `target_share`, `air_yards_share`, `wopr`, `ryoe`, `ryoe_per_att`, `epa_per_db`, `passing_cpoe`, `pass_adot`, `rec_adot`, `ff_xfp_raw`).
  - `main.player_week_xfp`: Raw `ff_opportunity` expected/actual components plus standard scoring presets (`xfp_ppr`, `xfp_half`, `xfp_std`, `xfp_yahoo`, `actual_*`, `luck_delta_*`, `has_ff_opp`), gracefully falling back when optional views (`pbp`, `snap_counts`, `ff_opportunity`) are absent.
  - `main.team_week_dvp`: Weekly defensive fantasy points and `xFP` allowed by `(defense_team, position, season, week)` across all 4 standard presets.
- **Read-Only External DB Attachments**: `connect(read_only=True, attach={"league": Path(...)})` attaches downstream SQLite or DuckDB databases strictly `READ_ONLY`.

## Installation

```bash
pip install -r requirements.txt
pip install -e . --no-deps --no-build-isolation
```

> In minimal Linux containers without `ensurepip` / `venv`, use `pip install --user -e .` (or `uv pip install -e .`). Outside a source checkout, set `NFL_WAREHOUSE_DIR` (or pass `--data-dir`) so `WarehousePaths.default()` knows where to store Parquet and DuckDB files.

## Quick Start

```bash
# Fast hydration excluding play-by-play (~66 MB Parquet / ~82 MB with DuckDB; recommended for 2-4 GiB CRA/CI)
python -m nfl_warehouse --profile lite

# Full core hydration including play-by-play (~413 MB Parquet / ~428 MB with DuckDB)
python -m nfl_warehouse --profile core

# Routine refresh (open seasons + unseasoned datasets)
python -m nfl_warehouse --refresh

# Include optional datasets (~445 MB Parquet / ~461 MB with DuckDB: FTN charting, PFR adv, participation, draft, combine, contracts)
python -m nfl_warehouse --profile full

# Custom scoring rulesets (replaces default presets; use a separate consumer DuckDB for additive overlays)
python -m nfl_warehouse --rebuild --scoring-config config/scoring_te_premium_ppfd.example.json

# Rebuild _manifest.json from existing Parquet files on disk (validates Parquet footers/schemas)
python -m nfl_warehouse --repair-manifest

# Point at an external or shared data directory
export NFL_WAREHOUSE_DIR=/mnt/data/nfl-warehouse
python -m nfl_warehouse --status
```

> **Concurrency note:** DuckDB enforces an exclusive write lock whenever updating views/tables and forbids mixing `read_only=True` and read-write connections inside the same process. Close any open `connect(read_only=True)` sessions before running `--refresh` or `--rebuild`.

## Connecting from Python

```python
from pathlib import Path
from nfl_warehouse import connect, query_polars

with connect(read_only=True) as con:
    top_wrs = query_polars(con, """
        SELECT p.season, p.week, p.player_name, p.team, p.points
        FROM main.player_week_points p
        WHERE p.site = 'ppr' AND p.season = 2025 AND p.position = 'WR'
        ORDER BY p.points DESC
        LIMIT 10
    """)  # or con.sql("...").pl() when pyarrow is installed

# Attach a downstream SQLite or DuckDB file strictly READ_ONLY:
with connect(read_only=True, attach={"my_league": Path("path/to/league.db")}) as con:
    ...
```

## Verification & Integrity Audit

```bash
# Fast offline unit tests (no network)
python -m unittest discover -s tests -v

# Live warehouse audit + hardware/disk telemetry
python scripts/audit_warehouse.py --profile lite --strict --telemetry-out docs/warehouse_telemetry.json
```

## Documentation

- [NFL Advanced Stats Glossary & SQL Reference](docs/nfl_advanced_stats_glossary.md)
- [Architecture & Multi-Repo Split Spec](docs/design_spec.md)
- [Agent Skill Reference](.agents/skills/nfl-stats-warehouse/SKILL.md)
