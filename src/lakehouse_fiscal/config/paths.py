from __future__ import annotations

from lakehouse_fiscal.config.profiles import Settings


def _join(root: str, *parts: str) -> str:
    return "/".join([root.rstrip("/"), *(part.strip("/") for part in parts)])


def table_location(layer: str, table: str, settings: Settings) -> str:
    return _join(settings.storage_root, layer, table)


def landing(prefix: str, settings: Settings) -> str:
    return _join(settings.storage_root, settings.landing_prefix, prefix)


def checkpoint(name: str, settings: Settings) -> str:
    return _join(settings.storage_root, settings.checkpoint_prefix, name)
