"""Portable path resolution for the NFL stats warehouse."""

from dataclasses import dataclass
import os
from pathlib import Path

ENV_WAREHOUSE_DIR = "NFL_WAREHOUSE_DIR"


def _package_repo_root() -> Path:
    """Return the repository root containing src/nfl_warehouse."""
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class WarehousePaths:
    """Canonical path registry for warehouse Parquet files, DuckDB, and configs."""

    root: Path
    data_dir: Path
    nflverse_dir: Path
    manifest_path: Path
    warehouse_path: Path
    config_dir: Path

    @classmethod
    def for_root(cls, root: Path | str) -> "WarehousePaths":
        """Build paths anchored at a repository or project root directory."""
        root_path = Path(root).expanduser().resolve()
        data_dir = root_path / "data"
        return cls.for_data_dir(
            data_dir=data_dir,
            config_dir=root_path / "config",
            root=root_path,
        )

    @classmethod
    def for_data_dir(
        cls,
        data_dir: Path | str,
        *,
        config_dir: Path | str | None = None,
        root: Path | str | None = None,
    ) -> "WarehousePaths":
        """Build paths anchored at an explicit data directory."""
        resolved_data = Path(data_dir).expanduser().resolve()
        repo_root = _package_repo_root()
        resolved_root = (
            Path(root).expanduser().resolve() if root is not None else repo_root
        )
        resolved_config = (
            Path(config_dir).expanduser().resolve()
            if config_dir is not None
            else repo_root / "config"
        )
        nflverse_dir = resolved_data / "nflverse"
        return cls(
            root=resolved_root,
            data_dir=resolved_data,
            nflverse_dir=nflverse_dir,
            manifest_path=nflverse_dir / "_manifest.json",
            warehouse_path=resolved_data / "nfl_stats.duckdb",
            config_dir=resolved_config,
        )

    @classmethod
    def default(cls) -> "WarehousePaths":
        """Resolve default paths, honoring the NFL_WAREHOUSE_DIR env override."""
        env_dir = os.environ.get(ENV_WAREHOUSE_DIR, "").strip()
        repo_root = _package_repo_root()
        if env_dir:
            return cls.for_data_dir(
                env_dir,
                config_dir=repo_root / "config",
                root=repo_root,
            )
        return cls.for_root(repo_root)


def default_scoring_config_path(paths: WarehousePaths) -> Path:
    """Return scoring.json inside paths.config_dir or fall back to package config."""
    candidate = paths.config_dir / "scoring.json"
    if candidate.is_file():
        return candidate
    return _package_repo_root() / "config" / "scoring.json"
