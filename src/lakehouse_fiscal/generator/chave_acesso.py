from __future__ import annotations


def _access_digit(base: str) -> str:
    total = 0
    weight = 2
    for char in reversed(base):
        total += int(char) * weight
        weight = 2 if weight == 9 else weight + 1
    digit = 11 - total % 11
    return "0" if digit >= 10 else str(digit)


def build_chave_acesso(
    *,
    uf: str,
    aamm: str,
    cnpj: str,
    modelo: str = "55",
    serie: int,
    numero: int,
    tipo_emissao: int = 1,
    codigo_numerico: int,
) -> str:
    base = (
        f"{uf:0>2}{aamm:0>4}{cnpj:0>14}{modelo:0>2}{serie:03d}"
        f"{numero:09d}{tipo_emissao}{codigo_numerico:08d}"
    )
    if len(base) != 43 or not base.isdigit():
        raise ValueError("NF-e access-key segments must form exactly 43 digits before the DV.")
    return base + _access_digit(base)


def is_valid_chave_acesso(value: str | None) -> bool:
    return bool(
        value and len(value) == 44 and value.isdigit() and value[-1] == _access_digit(value[:-1])
    )
