# `nfl-stats-warehouse` Architecture & Multi-Repo Split Spec

**Status:** Phase 1 Implemented  
**Repository:** `c:\github\agy\nfl-stats-warehouse` (Standalone Public Warehouse)

---

## 1. Multi-Repo Split Architecture

`mfl-recap` previously mixed a reusable public `nflverse` + DuckDB warehouse (`src/stats_db/`) with commissioned MFL `40694` SQLite history, Yahoo DFS `Jtrey Sports Net` contest scrapers, paid/cached FantasyPros/BetQL snapshots, and the FooBot Discord bot. Splitting the stack cleanly isolates:

1. **Repo 1 (`nfl-stats-warehouse`, Phase 1 — THIS REPO):**
   - 100% free, zero-API-key, public NFL analytics warehouse (`src/nfl_warehouse/`).
   - 25 cataloged `nflverse` Parquet datasets (`16` core including `team_stats_week` & `trades`, `9` optional including `pfr_adv_def`).
   - `lite` hydration profile (`--profile lite` / `--no-pbp`, ~72 MB vs ~385 MB) for ephemeral Cloud Remote Agents and CI.
   - Portable relative DuckDB `nfl.*` views resolved via `SET file_search_path` + `NFL_WAREHOUSE_DIR`.
   - Public `main.player_dim` (cross-platform IDs, no `latest_team`), `main.site_scoring`, `main.player_week_points` (Offense + K + TE Premium + PPFD), and `main.team_defense_week_points`.
   - Point-in-time backtesting guardrail layer (`mart.team_abbr_map`, `mart.nextgen_*_weekly`, `mart.depth_charts_weekly`, `mart.player_team_week`).
   - Automated live data-integrity audit & telemetry (`scripts/audit_warehouse.py`, `docs/warehouse_telemetry.json`).

2. **Repo 2 (`foobot`, Phase 2 — Downstream Consumer):**
   - Commissioned league SQLite databases & archives (`league_history.db`, `ancient_history.db`, `bot_state.db`, `legacy.*` 4 archived MFL snapshot tables, `config/mfl_ground_truth_owners.json`, `config/manager_dossier.json`).
   - Discord slash commands (`src/bot/`), P2P side-bets, IR audits, local LiteRT Gemma 4 `/ask`, weekly high-scorer recaps, and MNF Monte Carlo sweat sheets.
   - Custom MFL `40694` scoring overlay passed via `--scoring-config` (or built downstream into a consumer DuckDB).

3. **Future Yahoo DFS & External Feeds Repo (Phase 3 — Downstream Consumer):**
   - Private `yahoo_dfs_*` 6 tables & raw JSON caches (`Jtrey Sports Net`), `fp_rankings`, `fp_rankings_acc`, `betql_events`, `betql_props`, and `src/dfs_opt/` lineup optimizer.

---

## 2. Storage & Portability Contract

- **Parquet Layout (`$NFL_WAREHOUSE_DIR/nflverse/` or `data/nflverse/`):**
  - Seasonal datasets: `<dataset>/<season>.parquet`
  - Non-seasonal datasets: `<dataset>/latest.parquet`
  - Lifecycle manifest: `_manifest.json` (`GRACE_DAYS = 30` post-Super Bowl before seasonal finalization)
- **Portable DuckDB Views:**
  - `views.py` writes `read_parquet('nflverse/<dataset>/*.parquet', union_by_name = true)` (zero OS drive letters or absolute directory prefixes).
  - Both `create_views()` and `connect()` execute `SET file_search_path = '<data_dir_posix>'`, allowing `data/` to be copied between Windows and Linux containers and queried read-only from any CWD without `--rebuild`.

---

## 3. Phase 2 Handoff Contract (Extracting `c:\github\agy\foobot`)

When splitting `foobot` out of `mfl-recap` after the 2026 season (or next phase):

### How `foobot` Consumes `nfl-stats-warehouse`
1. **Depend on `nfl-stats-warehouse`** via `pip install -e ../nfl-stats-warehouse` or Git dependency (`NFL_WAREHOUSE_DIR` pointing at the shared or local warehouse `data/` directory).
2. **Open read-only with private attachments:**
   ```python
   from pathlib import Path
   from nfl_warehouse import WarehousePaths, connect

   con = connect(
       paths=WarehousePaths.default(),
       read_only=True,
       attach={
           "league": Path("data/league_history.db"),
           "ancient": Path("data/ancient_history.db"),
           "bot_state": Path("data/bot_state.db"),
       },
   )
   ```
   - `connect()` attaches all SQLite DBs strictly `READ_ONLY` and validates alias names.
   - For stateful bot operations (registrations, side-bets), `foobot` continues using its own `StateDB` SQLite connection directly while keeping DuckDB read-only.
3. **Custom MFL `40694` Scoring Overlay:**
   - Keep MFL `40694` specific rules (`floor_zero_stats: ["pass_yd", "rush_yd", "rec_yd"]`, `fum_lost_total: -2.0`, `fum_rec_td: 6.0`, 300/400/500/600 passing yardage bonuses) inside `foobot/config/mfl_scoring.json`.
   - Rebuild or compute MFL scoring cleanly via `nfl-warehouse --scoring-config path/to/mfl_scoring.json` or an attached consumer DuckDB / SQLite table so the public `nfl-stats-warehouse` repo stays 100% league-agnostic.

### Exact Code & Asset Inventory for Phase 2 (`foobot`)
- **Move into `foobot`:**
  - `src/bot/*`, `src/run_discord_bot.py`
  - `src/analytics.py`, `src/recap_context.py`, `src/generate_recap.py`, `src/sweat_engine.py`, `src/generate_sweat.py`, `src/playoff_engine.py`, `src/rivalry_engine.py`, `src/postseason_analyzer.py`, `src/contract_analyzer.py`
  - `src/mfl_client.py`, `src/db.py`, `src/backfill.py`, `src/ingest_playoffs.py`, `src/ingest_ancient_history.py`, `src/team_sync.py`, `src/publisher.py`
  - `src/stats_db/legacy.py` + `data/legacy/fantasy-football/` (`legacy.mfl_*` 4 tables)
  - `config/league_config.json`, `config/manager_dossier.json`, `config/mfl_ground_truth_owners.json`, `prompts/`, `reports/`
  - `.agents/skills/{mfl-weekly-recap,mfl-franchise-lineage,sports-satire-voice}/`
- **Keep separate for Phase 3 (`dfs-optimizer` / private feeds):**
  - `src/dfs_opt/*`, `src/run_dfs_pipeline.py`, `src/stats_db/yahoo_dfs.py`, `src/stats_db/fp_rankings.py`, `src/fantasypros_client.py`
  - `src/xfp_advisor/*`, `src/season_xfp.py`, `config/yahoo_dfs_league*.json`, `.agents/skills/{optimizing-yahoo-dfs,xfp-league-advisor}/`
