from __future__ import annotations

import argparse
import hashlib
import json
import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from faker import Faker

from lakehouse_fiscal.generator.chave_acesso import build_chave_acesso
from lakehouse_fiscal.generator.cnpj import generate_cnpj

_UFS = ("SP", "RJ", "MG", "PR", "SC", "RS", "BA", "PE", "CE", "GO")
_UF_CODES = {
    "SP": "35",
    "RJ": "33",
    "MG": "31",
    "PR": "41",
    "SC": "42",
    "RS": "43",
    "BA": "29",
    "PE": "26",
    "CE": "23",
    "GO": "52",
}
_CFOPS = ("5101", "5102", "6101", "6102", "5202", "6202")
_NCMS = ("84713012", "85176259", "94036000", "61091000", "39269090")


@dataclass(frozen=True)
class GeneratedVolume:
    invoices: int
    items: int
    events: int
    issuers: int
    products: int


@dataclass(frozen=True)
class GeneratedDataset:
    emitentes: tuple[dict[str, Any], ...]
    produtos: tuple[dict[str, Any], ...]
    nfe_cab: tuple[dict[str, Any], ...]
    nfe_item: tuple[dict[str, Any], ...]
    eventos: tuple[dict[str, Any], ...]
    volume: GeneratedVolume

    def stable_hash(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


def generate(seed: int = 42, scale: float = 1.0) -> GeneratedDataset:
    if scale <= 0:
        raise ValueError("scale must be positive")
    rng = random.Random(seed)
    faker = Faker("pt_BR")
    faker.seed_instance(seed)
    invoice_count = int(5_000 * scale)
    issuer_count = max(300, int(300 * scale))
    product_count = max(1_000, int(1_000 * scale))
    event_count = max(200, int(200 * scale))

    emitentes: list[dict[str, Any]] = []
    for index in range(issuer_count):
        uf = _UFS[index % len(_UFS)]
        emitentes.append(
            {
                "cnpj_emitente": generate_cnpj(rng),
                "razao_social": f"EMPRESA SINTETICA {index:04d} LTDA",
                "nome_fantasia": f"FISCAL DEMO {index:04d}",
                "regime_tributario": ("SIMPLES", "PRESUMIDO", "REAL")[index % 3],
                "uf": uf,
                "municipio": faker.city(),
                "cnae_principal": f"{rng.randrange(1000000, 9999999):07d}",
                "situacao_cadastral": "ATIVA",
            }
        )

    produtos: list[dict[str, Any]] = []
    for index in range(product_count):
        issuer = emitentes[index % issuer_count]
        produtos.append(
            {
                "cnpj_emitente": issuer["cnpj_emitente"],
                "codigo_produto": f"SKU-{index:06d}",
                "descricao_produto": f"PRODUTO SINTETICO {index:06d}",
                "ncm": _NCMS[index % len(_NCMS)],
                "cest": f"{rng.randrange(1000000, 9999999):07d}",
                "familia_produto": ("ELETRONICOS", "MOVEIS", "VESTUARIO", "UTILIDADES")[index % 4],
                "unidade_comercial": ("UN", "CX", "KG")[index % 3],
            }
        )

    start = datetime(2024, 1, 1, tzinfo=UTC)
    headers: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    for invoice_index in range(invoice_count):
        issuer = emitentes[invoice_index % issuer_count]
        issued_at = start + timedelta(minutes=invoice_index * 210)
        chave = build_chave_acesso(
            uf=_UF_CODES[str(issuer["uf"])],
            aamm=issued_at.strftime("%y%m"),
            cnpj=str(issuer["cnpj_emitente"]),
            serie=1 + invoice_index % 99,
            numero=invoice_index + 1,
            codigo_numerico=rng.randrange(100_000_000),
        )
        destination_uf = _UFS[(invoice_index * 7 + 3) % len(_UFS)]
        invoice_items: list[dict[str, Any]] = []
        for item_number in range(1, 11):
            product = produtos[(invoice_index * 10 + item_number - 1) % product_count]
            quantity = Decimal(1 + rng.randrange(5))
            unit_price = Decimal(rng.randrange(500, 50_000)) / Decimal(100)
            product_value = (quantity * unit_price).quantize(Decimal("0.01"))
            icms = (product_value * Decimal("0.18")).quantize(Decimal("0.01"))
            ipi = (product_value * Decimal("0.05")).quantize(Decimal("0.01"))
            pis = (product_value * Decimal("0.0165")).quantize(Decimal("0.01"))
            cofins = (product_value * Decimal("0.076")).quantize(Decimal("0.01"))
            invoice_items.append(
                {
                    "chave_acesso": chave,
                    "num_item": item_number,
                    **product,
                    "cfop": _CFOPS[(invoice_index + item_number) % len(_CFOPS)],
                    "quantidade": quantity,
                    "valor_unitario": unit_price,
                    "valor_produto": product_value,
                    "cst_icms": "00",
                    "csosn": None,
                    "valor_icms": icms,
                    "cst_ipi": "50",
                    "valor_ipi": ipi,
                    "cst_pis": "01",
                    "valor_pis": pis,
                    "cst_cofins": "01",
                    "valor_cofins": cofins,
                    "dh_emissao": issued_at,
                }
            )
        total = sum((row["valor_produto"] for row in invoice_items), Decimal(0))
        headers.append(
            {
                "chave_acesso": chave,
                "numero_nfe": invoice_index + 1,
                "serie": 1 + invoice_index % 99,
                "dh_emissao": issued_at,
                "cnpj_emitente": issuer["cnpj_emitente"],
                "cnpj_destinatario": generate_cnpj(rng),
                "uf_emitente": issuer["uf"],
                "uf_destinatario": destination_uf,
                "valor_total": total,
                "status_nfe": "AUTORIZADA",
                "updated_at_source": issued_at,
            }
        )
        items.extend(invoice_items)

    events = tuple(
        {
            "chave_acesso": headers[index]["chave_acesso"],
            "tipo_evento": "CANCELAMENTO",
            "sequencia_evento": 1,
            "dh_evento": headers[index]["dh_emissao"] + timedelta(days=1),
            "justificativa": "Cancelamento sintético para demonstração técnica",
        }
        for index in range(min(event_count, invoice_count))
    )
    return GeneratedDataset(
        emitentes=tuple(emitentes),
        produtos=tuple(produtos),
        nfe_cab=tuple(headers),
        nfe_item=tuple(items),
        eventos=events,
        volume=GeneratedVolume(invoice_count, len(items), len(events), issuer_count, product_count),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--out-postgres", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--out-landing", action=argparse.BooleanOptionalAction, default=False)
    return parser


def main() -> None:
    args = _parser().parse_args()
    dataset = generate(seed=args.seed, scale=args.scale)
    from lakehouse_fiscal.utils.logging import get_logger, log_json

    log_json(
        get_logger(__name__),
        "dataset_generated",
        **asdict(dataset.volume),
        hash=dataset.stable_hash(),
    )


if __name__ == "__main__":
    main()
