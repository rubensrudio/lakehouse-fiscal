from __future__ import annotations

from lakehouse_fiscal.generator.nfe_generator import generate


def test_small_generator_is_deterministic() -> None:
    first = generate(seed=42, scale=0.001)
    second = generate(seed=42, scale=0.001)
    assert first.stable_hash() == second.stable_hash()
    assert first.volume.invoices == 5
    assert first.volume.items == 50
    assert all(len(row["chave_acesso"]) == 44 for row in first.nfe_cab)
