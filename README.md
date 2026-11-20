# NFL Stats Warehouse

A portable, zero-API-key **DuckDB + nflverse Parquet** NFL analytics warehouse designed for local research, reproducible point-in-time backtesting, and ephemeral **Google Cloud Remote Agents** / CI sandboxes.

## Features

- **100% Public & Free**: Hydrates 25 `nflverse` datasets (`16` core + `9` optional) via `nflreadpy` with zero API keys or private credentials.
- **Fast `--profile lite` (~66 MB Parquet / ~82 MB with DuckDB, vs ~385 MB core)**: Skips heavy play-by-play (`pbp`) so cloud sandboxes and CI jobs hydrate the remaining core datasets in seconds. Note: peak hydration RAM (RSS) can reach ~1–2 GiB depending on OS (disk footprint != memory).
- **Zero Hardcoded OS Paths**: `nfl.*` views store relative Parquet paths resolved via `SET file_search_path`, making `data/` portable across Windows and Linux containers without running `--rebuild`.
- **Point-in-Time Backtesting Guardrails (`mart.*`)**:
  - `mart.team_abbr_map` normalizes `LAR -> LA`, `JAC -> JAX`, `WSH -> WAS`, and historical/PFR codes (`STL`, `SL`, `SD`, `OAK`, `ARZ`, `BLT`, `CLV`, `HST`).
  - `mart.nextgen_{passing,rushing,receiving}_weekly` excludes `week = 0` season-summary rows.
  - `mart.depth_charts_weekly` unifies pre-2025 `season`/`week` files and 2025+ timestamped `dt` snapshots using strictly pre-kickoff snapshots (`dt_ts < kickoff_ts` in `America/New_York`, within 14 days).
  - `mart.player_team_week` provides historical weekly team membership from `rosters_weekly` instead of static `players.latest_team`.
- **Pluggable Scoring & Crosswalk (`main.*`)**:
  - `main.player_dim` joins `gsis_id` to `mfl_id`, `yahoo_id`, `sleeper_id`, `espn_id`, `fantasypros_id`, `pfr_id`, `sportradar_id`, `rotowire_id`, and `pff_id`.
  - `main.player_week_points` and `main.team_defense_week_points` compute `ppr`, `half_ppr`, `standard`, and `yahoo` presets plus custom rules (`--scoring-config`, Kickers, Team DEF/DST, TE Premium, PPFD).
- **Read-Only External DB Attachments**: `connect(read_only=True, attach={"league": Path(...)})` attaches downstream SQLite or DuckDB databases strictly `READ_ONLY`.

## Installation

```bash
pip install -r requirements.txt
pip install -e . --no-deps --no-build-isolation
```

> In minimal Linux containers without `ensurepip` / `venv`, use `pip install --user -e .` (or `uv pip install -e .`). Outside a source checkout, set `NFL_WAREHOUSE_DIR` (or pass `--data-dir`) so `WarehousePaths.default()` knows where to store Parquet and DuckDB files.

## Quick Start

```bash
# Fast hydration excluding play-by-play (~66 MB Parquet / ~82 MB with DuckDB)
python -m nfl_warehouse --profile lite

# Full core hydration including play-by-play (~385 MB)
python -m nfl_warehouse --profile core

# Routine refresh (open seasons + unseasoned datasets)
python -m nfl_warehouse --refresh

# Include optional datasets (FTN charting, PFR adv pass/rush/rec/def, participation, draft, combine, contracts)
python -m nfl_warehouse --profile full

# Custom scoring rulesets (replaces default presets; use a separate consumer DuckDB for additive overlays)
python -m nfl_warehouse --rebuild --scoring-config config/scoring_te_premium_ppfd.example.json

# Point at an external or shared data directory
export NFL_WAREHOUSE_DIR=/mnt/data/nfl-warehouse
python -m nfl_warehouse --status
```

## Connecting from Python

```python
from pathlib import Path
from nfl_warehouse import connect

with connect(read_only=True) as con:
    top_wrs = con.execute("""
        SELECT p.season, p.week, p.player_name, p.team, p.points
        FROM main.player_week_points p
        WHERE p.site = 'ppr' AND p.season = 2025 AND p.position = 'WR'
        ORDER BY p.points DESC
        LIMIT 10
    """).pl()

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
