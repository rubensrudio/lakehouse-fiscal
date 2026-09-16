from __future__ import annotations

import random


def _digit(base: str, weights: tuple[int, ...]) -> str:
    total = sum(int(char) * weight for char, weight in zip(base, weights, strict=True))
    remainder = total % 11
    return "0" if remainder < 2 else str(11 - remainder)


def generate_cnpj(rng: random.Random) -> str:
    base = "".join(str(rng.randrange(10)) for _ in range(12))
    while len(set(base)) == 1:
        base = "".join(str(rng.randrange(10)) for _ in range(12))
    first = _digit(base, (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    second = _digit(base + first, (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    return base + first + second


def is_valid_cnpj(value: str | None) -> bool:
    if value is None or len(value) != 14 or not value.isdigit() or len(set(value)) == 1:
        return False
    first = _digit(value[:12], (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    second = _digit(value[:12] + first, (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    return value[-2:] == first + second
