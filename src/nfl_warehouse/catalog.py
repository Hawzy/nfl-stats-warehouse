"""Dataset catalog for public nflverse datasets."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Dataset:
    """Metadata specification for an ingestable nflverse dataset."""

    name: str
    loader: str
    loader_kwargs: tuple[tuple[str, object], ...] = ()
    seasonal: bool = True
    first_season: int | None = None
    optional: bool = False
    heavy: bool = False


VALID_PROFILES: tuple[str, ...] = ("lite", "core", "full")

CATALOG: tuple[Dataset, ...] = (
    # Core seasonal datasets (12)
    Dataset(
        name="pbp",
        loader="load_pbp",
        seasonal=True,
        first_season=1999,
        optional=False,
        heavy=True,
    ),
    Dataset(
        name="player_stats_week",
        loader="load_player_stats",
        loader_kwargs=(("summary_level", "week"),),
        seasonal=True,
        first_season=1999,
        optional=False,
    ),
    Dataset(
        name="team_stats_week",
        loader="load_team_stats",
        loader_kwargs=(("summary_level", "week"),),
        seasonal=True,
        first_season=1999,
        optional=False,
    ),
    Dataset(
        name="schedules",
        loader="load_schedules",
        seasonal=True,
        first_season=1999,
        optional=False,
    ),
    Dataset(
        name="rosters_weekly",
        loader="load_rosters_weekly",
        seasonal=True,
        first_season=2002,
        optional=False,
    ),
    Dataset(
        name="snap_counts",
        loader="load_snap_counts",
        seasonal=True,
        first_season=2013,
        optional=False,
    ),
    Dataset(
        name="injuries",
        loader="load_injuries",
        seasonal=True,
        first_season=2009,
        optional=False,
    ),
    Dataset(
        name="depth_charts",
        loader="load_depth_charts",
        seasonal=True,
        first_season=2001,
        optional=False,
    ),
    Dataset(
        name="nextgen_passing",
        loader="load_nextgen_stats",
        loader_kwargs=(("stat_type", "passing"),),
        seasonal=True,
        first_season=2016,
        optional=False,
    ),
    Dataset(
        name="nextgen_rushing",
        loader="load_nextgen_stats",
        loader_kwargs=(("stat_type", "rushing"),),
        seasonal=True,
        first_season=2016,
        optional=False,
    ),
    Dataset(
        name="nextgen_receiving",
        loader="load_nextgen_stats",
        loader_kwargs=(("stat_type", "receiving"),),
        seasonal=True,
        first_season=2016,
        optional=False,
    ),
    Dataset(
        name="ff_opportunity",
        loader="load_ff_opportunity",
        seasonal=True,
        first_season=2006,
        optional=False,
    ),
    # Core non-seasonal datasets (4)
    Dataset(
        name="players",
        loader="load_players",
        seasonal=False,
        first_season=None,
        optional=False,
    ),
    Dataset(
        name="ff_playerids",
        loader="load_ff_playerids",
        seasonal=False,
        first_season=None,
        optional=False,
    ),
    Dataset(
        name="teams",
        loader="load_teams",
        seasonal=False,
        first_season=None,
        optional=False,
    ),
    Dataset(
        name="trades",
        loader="load_trades",
        seasonal=False,
        first_season=None,
        optional=False,
    ),
    # Optional seasonal datasets (6)
    Dataset(
        name="participation",
        loader="load_participation",
        seasonal=True,
        first_season=2016,
        optional=True,
    ),
    Dataset(
        name="ftn_charting",
        loader="load_ftn_charting",
        seasonal=True,
        first_season=2022,
        optional=True,
    ),
    Dataset(
        name="pfr_adv_pass",
        loader="load_pfr_advstats",
        loader_kwargs=(("stat_type", "pass"), ("summary_level", "week")),
        seasonal=True,
        first_season=2018,
        optional=True,
    ),
    Dataset(
        name="pfr_adv_rush",
        loader="load_pfr_advstats",
        loader_kwargs=(("stat_type", "rush"), ("summary_level", "week")),
        seasonal=True,
        first_season=2018,
        optional=True,
    ),
    Dataset(
        name="pfr_adv_rec",
        loader="load_pfr_advstats",
        loader_kwargs=(("stat_type", "rec"), ("summary_level", "week")),
        seasonal=True,
        first_season=2018,
        optional=True,
    ),
    Dataset(
        name="pfr_adv_def",
        loader="load_pfr_advstats",
        loader_kwargs=(("stat_type", "def"), ("summary_level", "week")),
        seasonal=True,
        first_season=2018,
        optional=True,
    ),
    # Optional non-seasonal datasets (3)
    Dataset(
        name="draft_picks",
        loader="load_draft_picks",
        seasonal=False,
        first_season=None,
        optional=True,
    ),
    Dataset(
        name="combine",
        loader="load_combine",
        seasonal=False,
        first_season=None,
        optional=True,
    ),
    Dataset(
        name="contracts",
        loader="load_contracts",
        seasonal=False,
        first_season=None,
        optional=True,
    ),
)


def get_datasets(
    include_optional: bool = False,
    names: list[str] | None = None,
    *,
    profile: str | None = None,
    exclude_pbp: bool = False,
) -> list[Dataset]:
    """Return datasets filtered by profile, optionality, or explicit names."""
    if profile is not None and profile not in VALID_PROFILES:
        raise ValueError(
            f"Unknown profile {profile!r}; expected one of {', '.join(VALID_PROFILES)}"
        )

    catalog_by_name = {d.name: d for d in CATALOG}
    if names is not None:
        unknown = [name for name in names if name not in catalog_by_name]
        if unknown:
            raise ValueError(f"Unknown dataset(s): {', '.join(unknown)}")
        selected = [catalog_by_name[name] for name in names]
    else:
        eff_profile = profile
        if eff_profile is None:
            eff_profile = "full" if include_optional else "core"
        elif include_optional and eff_profile == "core":
            eff_profile = "full"

        if eff_profile == "lite":
            selected = [d for d in CATALOG if not d.optional and not d.heavy]
        elif eff_profile == "core":
            selected = [d for d in CATALOG if not d.optional]
        else:
            selected = list(CATALOG)

    if exclude_pbp:
        selected = [d for d in selected if d.name != "pbp"]
    return selected
