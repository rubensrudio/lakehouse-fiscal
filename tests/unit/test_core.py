from __future__ import annotations

import random
from dataclasses import FrozenInstanceError

import pytest

from lakehouse_fiscal.config.naming import LOGICAL_TABLES, InvalidIdentifierError, parse, resolve
from lakehouse_fiscal.config.profiles import InvalidProfileError, Profile, Settings, load_settings
from lakehouse_fiscal.generator.chave_acesso import build_chave_acesso, is_valid_chave_acesso
from lakehouse_fiscal.generator.cnpj import generate_cnpj, is_valid_cnpj
from lakehouse_fiscal.utils.ulid import new_ulid


def _settings(profile: Profile) -> Settings:
    return Settings(
        profile, "lakehouse_fiscal", "root", "landing", "checkpoints", "jdbc", "scope", 42, 168, 100
    )


def test_naming_inventory_round_trip() -> None:
    assert sum(map(len, LOGICAL_TABLES.values())) == 20
    for profile in Profile:
        settings = _settings(profile)
        for layer, tables in LOGICAL_TABLES.items():
            for table in tables:
                assert parse(resolve(layer, table, settings), settings) == (
                    settings.catalog,
                    layer,
                    table,
                )


def test_naming_rejects_unknown() -> None:
    with pytest.raises(InvalidIdentifierError):
        resolve("gold", "missing", _settings(Profile.LOCAL))


def test_profiles_are_explicit_and_frozen(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKEHOUSE_PROFILE", raising=False)
    with pytest.raises(InvalidProfileError):
        load_settings()
    monkeypatch.setenv("LAKEHOUSE_PROFILE", "local")
    settings = load_settings()
    with pytest.raises(FrozenInstanceError):
        settings.catalog = "changed"  # type: ignore[misc]


def test_cnpj_is_deterministic_and_valid() -> None:
    left, right = random.Random(42), random.Random(42)
    values = [generate_cnpj(left) for _ in range(1_000)]
    assert values == [generate_cnpj(right) for _ in range(1_000)]
    assert all(is_valid_cnpj(value) for value in values)
    assert not is_valid_cnpj("11111111111111")


def test_access_key_validation() -> None:
    key = build_chave_acesso(
        uf="35", aamm="2401", cnpj="11222333000181", serie=1, numero=42, codigo_numerico=12345678
    )
    assert len(key) == 44
    assert is_valid_chave_acesso(key)
    assert not is_valid_chave_acesso(key[:-1] + str((int(key[-1]) + 1) % 10))


def test_ulid_shape_and_sortability() -> None:
    first = new_ulid()
    second = new_ulid()
    assert len(first) == 26
    assert set(first) <= set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
    assert first <= second or first[:10] == second[:10]
