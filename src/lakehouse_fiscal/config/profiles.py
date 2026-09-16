from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import yaml


class InvalidProfileError(ValueError):
    """Raised when LAKEHOUSE_PROFILE is missing or unsupported."""


class Profile(StrEnum):
    LOCAL = "local"
    DATABRICKS = "databricks"


@dataclass(frozen=True)
class Settings:
    profile: Profile
    catalog: str
    storage_root: str
    landing_prefix: str
    checkpoint_prefix: str
    jdbc: str
    secret_scope: str
    seed: int
    min_vacuum_retain_hours: int
    max_files_per_trigger: int


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_settings() -> Settings:
    raw_profile = os.environ.get("LAKEHOUSE_PROFILE")
    try:
        profile = Profile(raw_profile or "")
    except ValueError as exc:
        message = (
            f"Invalid LAKEHOUSE_PROFILE='{raw_profile}'. Valid values: 'local', 'databricks'. "
            "Set it in .env or export it before running."
        )
        raise InvalidProfileError(message) from exc
    config_path = _project_root() / "conf" / f"{profile.value}.yml"
    values = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    return Settings(
        profile=profile,
        catalog=str(values["catalog"]),
        storage_root=str(values["storage_root"]),
        landing_prefix=str(values["landing_prefix"]),
        checkpoint_prefix=str(values["checkpoint_prefix"]),
        jdbc=str(values["jdbc"]),
        secret_scope=str(values["secret_scope"]),
        seed=int(values["seed"]),
        min_vacuum_retain_hours=int(values["min_vacuum_retain_hours"]),
        max_files_per_trigger=int(values["max_files_per_trigger"]),
    )
