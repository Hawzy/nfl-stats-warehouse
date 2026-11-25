# NFL Advanced Stats Glossary & DuckDB SQL Reference

A public reference guide mapping modern NFL analytics metrics to exact **nflverse** tables, columns, interpretations, lookahead guardrails, and runnable DuckDB SQL queries inside `nfl-stats-warehouse`.

---

## Quick Orientation: Warehouse Schemas

| Schema | Role | Examples |
|---|---|---|
| `nfl.*` | Raw nflverse Parquet views (`union_by_name = true`) | `nfl.player_stats_week`, `nfl.team_stats_week`, `nfl.pbp`, `nfl.ff_opportunity`, `nfl.snap_counts` |
| `main.*` | Curated dimension, scoring & Gold analytical tables | `main.player_dim`, `main.player_week_points`, `main.team_defense_week_points`, `main.site_scoring`, `main.player_week_adv`, `main.player_week_xfp`, `main.team_week_dvp` |
| `mart.*` | Point-in-time backtesting guardrail layer | `mart.team_abbr_map`, `mart.nextgen_*_weekly`, `mart.depth_charts_weekly`, `mart.player_team_week` |

---

## 0. Materialized Gold Analytical Tables (`main.player_week_adv`, `main.player_week_xfp`, `main.team_week_dvp`)

Automatically materialized at the end of `python -m nfl_warehouse` (`build_advanced_stats_tables(con)`) at deduplicated `REG`-season `(gsis_id, season, week)` / `(defense_team, position, season, week)` grain:

- **`main.player_week_adv`**: Usage and efficiency metrics (`snap_pct`, `offense_snaps`, `team_dropbacks`, `eff_dropbacks`, `est_routes`, `constrained_routes`, `yprr`, `yprr_constrained`, `tprr_pct`, `target_share`, `air_yards_share`, `wopr`, `rec_adot`, `ryoe`, `ryoe_per_att`, `epa_per_db`, `passing_cpoe`, `pass_adot`, `ff_xfp_raw`). Uses frozen pre-2025 `FROZEN_ROUTE_BETAS` (no runtime dependency on post-season `nfl.participation` or `nfl.ftn_charting`). Under `--profile lite` (no `nfl.pbp`), `team_dropbacks` is `0.0` while `eff_dropbacks` falls back to `player_stats_week` team dropbacks (or `34.0`) so `constrained_routes` and `yprr_constrained` remain available.
- **`main.player_week_xfp`**: Raw expected and actual stat components (`pass_yds_exp`, `pass_td_exp`, `pass_int_exp`, `rush_yds_exp`, `rush_td_exp`, `rush_attempts_exp`, `rec_yds_exp`, `receptions_exp`, `rec_td_exp`, `two_pt_exp`, plus actual box columns and `sacks_suffered`) alongside league-agnostic presets (`xfp_ppr`, `xfp_half`, `xfp_std`, `xfp_yahoo`, `actual_ppr`, `actual_half`, `actual_std`, `actual_yahoo`, `luck_delta_ppr`, `luck_delta_half`, `luck_delta_std`, `luck_delta_yahoo`, `has_ff_opp`). Uses pre-2026 `EMPIRICAL_FUMBLE_LOST_RATE` for expected fumble penalties and falls back to `actual_*` when `nfl.ff_opportunity` is absent for a player-week (`has_ff_opp = FALSE`). Downstream league consumers compute custom-scoring `xFP` in a separate overlay database directly from the raw component columns.
- **`main.team_week_dvp`**: Weekly Defense vs. Position (`defense_team`, `offense_team`, `position`, `season`, `week`, `player_count`) aggregating `pts_allowed_{ppr,half,std,yahoo}` and `xfp_allowed_{ppr,half,std,yahoo}` from `main.player_week_xfp`.
- **Missing Optional Views**: When `nfl.player_stats_week` is absent, all three tables are created as empty typed schemas; when optional views (`pbp`, `snap_counts`, `ff_opportunity`, `nextgen_rushing`, `participation`, `ftn_charting`) are absent, builders degrade cleanly with `0.0` / `FALSE` fallbacks without failing.

---

## 1. Expected Points Added (EPA) & Win Probability Added (WPA)

### What It Measures
- **EPA (`epa`)** measures how many expected points a play adds or subtracts based on down, distance, field position, score differential, and time remaining before vs. after the snap. Unlike raw yardage, a 4-yard rush on 3rd-and-3 (`+1.2 EPA`) is worth far more than a 12-yard completion on 3rd-and-20 (`-0.4 EPA`).
- **QB EPA (`qb_epa`)** assigns play EPA to the quarterback including scrambles and sacks; **Air EPA (`air_epa`)** and **YAC EPA (`yac_epa`)** split passing value into throw distance vs. yards after catch.
- **WPA (`wpa`)** measures the change in win probability from before to after the play.

### Exact Warehouse Locations
- **Weekly Player Box Aggregates (`nfl.player_stats_week`)**: `passing_epa`, `rushing_epa`, `receiving_epa`
- **Weekly Team Aggregates (`nfl.team_stats_week`)**: `passing_epa`, `rushing_epa`
- **Play-by-Play (`nfl.pbp`)**: `epa`, `qb_epa`, `air_epa`, `yac_epa`, `wpa`

### Interpretation Benchmarks
- **Passing EPA / Play (`pbp`)**: Elite QBs typically post `+0.18` to `+0.30`; league average sits near `+0.02` to `+0.06`.
- **Rushing EPA / Play**: Because most runs gain fewer yards than passing downs, `+0.00` to `+0.05` is strong positive efficiency for an RB offense.

### Example SQL: Top QBs by Total Passing + Rushing EPA (Single Season)
```sql
SELECT
    p.player_id,
    d.display_name,
    COUNT(*) AS games,
    SUM(p.attempts) AS att,
    ROUND(SUM(p.passing_epa), 1) AS pass_epa,
    ROUND(SUM(p.rushing_epa), 1) AS rush_epa,
    ROUND(SUM(p.passing_epa + coalesce(p.rushing_epa, 0)), 1) AS total_epa
FROM nfl.player_stats_week p
JOIN main.player_dim d ON d.gsis_id = p.player_id
WHERE p.season = 2025
  AND p.season_type = 'REG'
  AND p.position = 'QB'
GROUP BY 1, 2
HAVING SUM(p.attempts) >= 200
ORDER BY total_epa DESC
LIMIT 15;
```

---

## 2. Completion Percentage Over Expected (CPOE)

### What It Measures
**CPOE** compares a quarterback's actual completion rate to the probability that an average QB completes those exact passes given depth of target, receiver separation, sideline proximity, pressure, and throw trajectory. It isolates ball placement and accuracy from scheme difficulty (a checkdown artist with 72% completions on 3-yard throws can still post negative CPOE).

### Exact Warehouse Locations
- `nfl.player_stats_week`: `passing_cpoe`
- `mart.nextgen_passing_weekly` / `nfl.nextgen_passing`: `completion_percentage`, `expected_completion_percentage`, `completion_percentage_above_expectation`
- `nfl.pbp`: `cpoe` (play-level)

### Interpretation Benchmarks
- **Elite**: `+3.5%` to `+6.0%` over a full season
- **Average**: `-1.0%` to `+1.5%`
- **Struggling**: `< -3.0%`

---

## 3. Air Yards, aDOT, PACR & RACR

### What They Measure
- **Air Yards**: Yards the ball travels through the air from the line of scrimmage to the target point (regardless of completion).
- **aDOT (Average Depth of Target)**: `receiving_air_yards / targets` for pass catchers, or `avg_intended_air_yards` in NextGen Stats. Distinguishes deep threats (`13+ yd aDOT`) from slot/underneath volume targets (`6–8 yd aDOT`).
- **PACR (Passing Air Conversion Ratio)**: `passing_yards / passing_air_yards` — how efficiently a QB turns thrown air yards into actual passing yards.
- **RACR (Receiver Air Conversion Ratio)**: `receiving_yards / receiving_air_yards` — how efficiently a receiver turns targeted air yards into receiving yards (combining catch rate + YAC). Note: negative air yards on screens can make single-week RACR negative or noisy; filter `SUM(receiving_air_yards) >= 100` across multi-week windows.

### Exact Warehouse Locations
- `nfl.player_stats_week`: `passing_air_yards`, `receiving_air_yards`, `pacr`, `racr`, `air_yards_share`
- `mart.nextgen_passing_weekly`: `avg_intended_air_yards`, `avg_completed_air_yards`, `avg_air_yards_to_sticks`, `avg_air_yards_differential`
- `mart.nextgen_receiving_weekly`: `avg_intended_air_yards`, `percent_share_of_intended_air_yards`
- `nfl.pbp`: `air_yards`, `yards_after_catch`

---

## 4. Target Share, Air Yards Share & WOPR

### What They Measure
- **Target Share (`target_share`)**: Player targets divided by total team pass attempts while active.
- **Air Yards Share (`air_yards_share`)**: Player receiving air yards divided by team passing air yards.
- **WOPR (Weighted Opportunity Rating)**: Josh Hermsmeyer's composite volume metric weighting targets (which drive PPR floors) and air yards (which drive yardage and TD ceilings):

$$\text{WOPR} = 1.5 \times \text{Target Share} + 0.7 \times \text{Air Yards Share}$$

### Exact Warehouse Locations
- `nfl.player_stats_week`: `targets`, `target_share`, `air_yards_share`, `wopr`

### Interpretation Benchmarks (WR Weekly / Rolling)
- **Alpha WR1**: `target_share >= 0.25`, `wopr >= 0.65`
- **Strong WR2 / Top TE**: `wopr 0.45–0.64`
- **Boom-Bust Deep Threat**: high `air_yards_share` (`> 0.30`) with modest `target_share` (`< 0.16`)

### Example SQL: Rolling Last-4-Weeks WR Usage Leaders
```sql
WITH recent AS (
    SELECT
        p.player_id,
        d.display_name,
        p.team,
        p.week,
        p.targets,
        p.receptions,
        p.receiving_yards,
        p.receiving_air_yards,
        p.target_share,
        p.air_yards_share,
        p.wopr
    FROM nfl.player_stats_week p
    JOIN main.player_dim d ON d.gsis_id = p.player_id
    WHERE p.season = 2025
      AND p.season_type = 'REG'
      AND p.position = 'WR'
      AND p.week BETWEEN 9 AND 12
)
SELECT
    display_name,
    MAX(team) AS team,
    COUNT(*) AS gms,
    SUM(targets) AS tgts,
    SUM(receiving_yards) AS rec_yds,
    ROUND(AVG(target_share) * 100, 1) AS avg_tgt_pct,
    ROUND(AVG(air_yards_share) * 100, 1) AS avg_ay_pct,
    ROUND(AVG(wopr), 3) AS avg_wopr
FROM recent
GROUP BY player_id, display_name
HAVING COUNT(*) >= 3
ORDER BY avg_wopr DESC
LIMIT 20;
```

---

## 5. Expected Fantasy Points (`xFP`) & Regression Delta

### What It Measures
`nfl.ff_opportunity` models what an **average NFL player** would score given the exact down, distance, field position, target depth, and carry location of every touch. Comparing actual fantasy points to `xFP` separates **role** (sticky, repeatable opportunity) from **efficiency / TD luck** (noisy, mean-reverting):
- **Negative `diff` (Actual < Expected)**: **Buy-Low / Positive Regression** candidate — getting high-value red-zone/deep volume that hasn't converted yet.
- **Positive `diff` (Actual > Expected)**: **Sell-High / Regression Risk** — scoring above volume expectation via unsustainable TD rates or long broken plays (elite efficiency talents can sustain positive deltas, though extreme `+5 PPG` gaps rarely hold).

### Exact Warehouse Locations (`nfl.ff_opportunity`)
- Expected points: `total_fantasy_points_exp`, `pass_fantasy_points_exp`, `rec_fantasy_points_exp`, `rush_fantasy_points_exp`
- Actual vs. Expected: `total_fantasy_points`, `total_fantasy_points_diff`, `rec_fantasy_points_diff`, `rush_fantasy_points_diff`
- Team context totals: `*_team` columns (e.g., `total_fantasy_points_exp_team`, `rec_attempt_team`)

> **Schema Caveat:** `nfl.ff_opportunity` uses `posteam` (not `team`) and `total_fantasy_points` reflects nflverse's standard PPR expectation baseline. It also includes team-level unattributed rows where `player_id IS NULL` (e.g., throwaways and spikes); filter `WHERE player_id IS NOT NULL` when joining to `main.player_dim` (100% match on non-null IDs).

### Example SQL: Season Buy-Low & Sell-High Skill Players (`ff_opportunity`)
```sql
SELECT
    o.player_id,
    MAX(o.full_name) AS player_name,
    MAX(o.position) AS pos,
    COUNT(*) AS games,
    ROUND(AVG(o.total_fantasy_points_exp), 2) AS xfp_pg,
    ROUND(AVG(o.total_fantasy_points), 2) AS actual_fp_pg,
    ROUND(AVG(o.total_fantasy_points_diff), 2) AS luck_delta_pg
FROM nfl.ff_opportunity o
WHERE o.season = 2025
  AND o.week <= 17
  AND o.position IN ('RB', 'WR', 'TE')
GROUP BY o.player_id
HAVING COUNT(*) >= 6 AND AVG(o.total_fantasy_points_exp) >= 10.0
ORDER BY luck_delta_pg ASC
LIMIT 15;
```

---

## 6. NextGen Rushing: RYOE, Efficiency & Stacked Boxes

### What They Measure
- **RYOE (`rush_yards_over_expected`, `rush_yards_over_expected_per_att`)**: Actual rushing yards minus expected rushing yards from player-tracking blocker leverage and defender velocity at handoff. Isolates RB vision and elusiveness from offensive line blocking.
- **8+ Defenders in Box (`percent_attempts_gte_eight_defenders`)**: Percentage of carries facing heavy run boxes (often high for early-down/goal-line backs or offenses lacking a passing threat).
- **Efficiency (`efficiency`)**: Total distance traveled by the ball carrier divided by net yards gained — lower values indicate decisive north-south running (`< 3.5`), while high values indicate east-west dancing behind the line (`> 4.5`).

### Exact Warehouse Locations (`mart.nextgen_rushing_weekly` — Guarded)
Use `mart.nextgen_rushing_weekly` (strips `week = 0` season rollups and normalizes `LAR -> LA`):
- `rush_yards_over_expected`, `rush_yards_over_expected_per_att`, `rush_pct_over_expected`, `expected_rush_yards`, `percent_attempts_gte_eight_defenders`, `efficiency`, `avg_time_to_los`

---

## 7. NextGen Receiving: Separation, Cushion & YAC Over Expected

### What They Measure
- **Separation (`avg_separation`)**: Yards between the receiver and nearest defender at the moment of catch/incompletion (`~2.8–3.2 yds` is average; `> 3.5 yds` reflects elite route-running or scheme space; contested-catch WRs often live around `2.0–2.5 yds`).
- **Cushion (`avg_cushion`)**: Yards of pre-snap/snap-point space given by the nearest DB (`> 6.5 yds` signals soft zone / deep respect).
- **YAC Over Expected (`avg_yac_above_expectation`)**: Actual YAC minus tracking-model expected YAC based on defender angles and speed at the catch point (`avg_yac - avg_expected_yac`).

### Exact Warehouse Locations (`mart.nextgen_receiving_weekly` — Guarded)
- `avg_separation`, `avg_cushion`, `avg_intended_air_yards`, `percent_share_of_intended_air_yards`, `catch_percentage`, `avg_yac`, `avg_expected_yac`, `avg_yac_above_expectation`

---

## 8. Snap Share & Route Participation Proxy

### What It Measures
- **Snap Share (`offense_pct`)**: Percentage of team offensive snaps played by a player (`offense_snaps` / team total offensive snaps).
- **Join Rule**: `nfl.snap_counts` is keyed by **`pfr_player_id`**, NOT `gsis_id`. Always bridge through `main.player_dim.pfr_id = snap_counts.pfr_player_id` AND canonical `team` (~0.12% upstream `pfr_id` gap on sparse/reserve players; non-skill/defensive/OL players appear in `snap_counts` but have no offensive `player_week_points` rows).

### Example SQL: Join `player_week_points` -> `player_dim` -> `nfl.snap_counts`
```sql
SELECT
    p.season,
    p.week,
    p.player_name,
    p.position,
    p.team,
    p.points AS ppr_pts,
    sc.offense_snaps,
    ROUND(sc.offense_pct * 100, 1) AS snap_pct
FROM main.player_week_points p
JOIN main.player_dim d ON d.gsis_id = p.player_id
JOIN nfl.snap_counts sc
  ON sc.pfr_player_id = d.pfr_id
 AND sc.season = p.season
 AND sc.week = p.week
 AND sc.game_type = p.season_type
LEFT JOIN mart.team_abbr_map m
  ON upper(trim(sc.team)) = m.raw_abbr
WHERE p.site = 'ppr'
  AND p.season = 2025
  AND p.week = 10
  AND p.position IN ('RB', 'WR', 'TE')
  AND COALESCE(m.team, sc.team) = p.team
ORDER BY p.points DESC
LIMIT 15;
```

---

## 9. PFR Advanced Metrics (Optional `--profile full`)

Hydrated with `--profile full` (or `--datasets pfr_adv_pass pfr_adv_rush pfr_adv_rec pfr_adv_def`):
- **`nfl.pfr_adv_pass`**: `times_pressured`, `times_pressured_pct`, `times_blitzed`, `times_hurried`, `times_hit`, `passing_drops`, `passing_drop_pct`, `passing_bad_throws`, `passing_bad_throw_pct`
- **`nfl.pfr_adv_rush`**: `rushing_yards_before_contact`, `rushing_yards_before_contact_avg`, `rushing_yards_after_contact`, `rushing_yards_after_contact_avg`, `rushing_broken_tackles`
- **`nfl.pfr_adv_rec`**: `receiving_broken_tackles`, `receiving_drop`, `receiving_drop_pct`, `receiving_int`, `receiving_rat` (passer rating when targeted)
- **`nfl.pfr_adv_def`**: `def_targets`, `def_completions_allowed`, `def_completion_pct`, `def_yards_allowed`, `def_yards_allowed_per_tgt`, `def_receiving_td_allowed`, `def_passer_rating_allowed`, `def_adot`, `def_pressures`, `def_sacks`, `def_missed_tackle_pct`

---

## 10. FTN Charting Metrics (Optional `--profile full`)

Manually charted play-level context from 2022+ (`nfl.ftn_charting`, joined to `nfl.pbp` via `nflverse_game_id = game_id` and `nflverse_play_id = play_id`):
- **Pre-snap & Play Concept**: `qb_location`, `n_offense_backfield`, `n_defense_box`, `is_no_huddle`, `is_motion`, `is_play_action`, `is_screen_pass`, `is_rpo`, `is_trick_play`
- **Pass Quality & Coverage**: `is_qb_out_of_pocket`, `is_interception_worthy`, `is_throw_away`, `is_catchable_ball`, `is_contested_ball`, `is_created_reception`, `is_drop`, `n_blitzers`, `n_pass_rushers`, `is_qb_fault_sack`

---

## 11. Point-in-Time Backtesting Rules & Known Data Quirks

| Trap / Quirk | What Happens in Raw `nfl.*` | Warehouse Solution |
|---|---|---|
| **1. NextGen `week = 0` leakage** | `nfl.nextgen_*` mixes `week = 0` full-season rollups into weekly rows. | Query `mart.nextgen_{passing,rushing,receiving}_weekly` (`week >= 1`). |
| **2. `nfl.participation` backfill** | `participation` is published post-season as a backfill dataset and has **no `season` or `week` columns** (keyed only by `nflverse_game_id` and `play_id`). | Never reference `nfl.participation` in predictive pre-game features; for post-hoc research, join on `(nflverse_game_id, play_id)` to `nfl.pbp` / `nfl.schedules` for season/week. |
| **3. `depth_charts` 2025+ split & multi-slot grain** | Starting 2025, `season`/`week`/`club_code`/`depth_team` are `NULL`, replaced by ISO `dt` timestamps, `team`, `pos_abb`, `pos_rank`, and new `pos_grp` values; players with multiple slots (`WR` + `KR`/`PR`) appear multiple times per week. | Use `mart.player_depth_rank_week` for 1:1 `(season, week, team, gsis_id)` joins, or `mart.depth_charts_weekly` (filter by normalized `unit` / `position`) for slot-level pre-kickoff views. |
| **4. Static `nfl.players.latest_team` & gameday `INA`** | `nfl.players` leaks mid-season trades & retirements; additionally `mart.player_team_week.status` (`INA`) reflects ~90m pre-kickoff inactive lists (0% in unplayed live weeks). | Use `mart.player_team_week.team` (and `position`) for pre-kickoff historical team/position; never feed `status = 'INA'` into pre-gameday predictive features. |
| **5. Static `player_stats_week.position`** | `nfl.player_stats_week` stamps a player's latest career position across all seasons (e.g., Taysom Hill `TE`, Travis Hunter `CB`). | `main.player_week_points` scores all non-`K` players with offensive/ST-return stats; use `mart.player_team_week.position` for historical week position. |
| **6. Team abbreviation drift** | NextGen uses `LAR`; legacy rosters use `ARZ`/`BLT`/`CLV`/`HST`/`SL`; schedules pre-relocation use `STL`/`SD`/`OAK`. | Join on `mart.team_abbr_map` (already pre-applied in all `mart.*` views and `main.team_defense_week_points`). |
| **7. Non-offense fumbles lost** | nflverse `fantasy_points_ppr` counts only `rushing + receiving + sack` fumbles lost, omitting kick/punt return or recovery fumbles captured by `fumbles_lost_total`. | `scoring.py` uses `fum_lost` for exact 0-diff `ppr` parity with nflverse and `fum_lost_total` for `yahoo`. |
| **8. 2025+ `yahoo_id` upstream gap** | Upstream `nflverse/ff_playerids` stopped populating `yahoo_id` starting with the 2025 draft class. | Join via `gsis_id`, `mfl_id`, `sleeper_id`, `espn_id`, or `fantasypros_id` + normalized name for 2025+ rookies. |
| **9. Upstream `injuries`, `schedules` & `trades` caveats** | `nfl.injuries.date_modified` is `NULL` in 2025+ (and earlier years can have duplicate statuses or rare post-kick TNF timestamps). `nfl.schedules` `spread_line` is closing-only with `+` = home favored; `temp`/`wind` have blackouts in `2022 W1–W8` and `2026 W5–W11`. `nfl.trades` stops at `2026-04-17`. | Deduplicate `nfl.injuries` with `(date_modified IS NULL OR date_modified < kickoff_ts)`, negate `spread_line` if matching Vegas home-minus convention, and drop `home_score`/`away_score` from feature tables. |
