from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from typing import Any

import psycopg

from lakehouse_fiscal.config.secrets import get_secret
from lakehouse_fiscal.generator.nfe_generator import generate
from lakehouse_fiscal.utils.logging import get_logger, log_json

DDL = """
CREATE TABLE IF NOT EXISTS emitente (
  cnpj_emitente CHAR(14) PRIMARY KEY, razao_social TEXT NOT NULL, nome_fantasia TEXT NOT NULL,
  regime_tributario TEXT NOT NULL, uf CHAR(2) NOT NULL, municipio TEXT NOT NULL,
  cnae_principal CHAR(7) NOT NULL, situacao_cadastral TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS destinatario (cnpj_destinatario CHAR(14) PRIMARY KEY);
CREATE TABLE IF NOT EXISTS produto (
  cnpj_emitente CHAR(14) NOT NULL, codigo_produto TEXT NOT NULL, descricao_produto TEXT NOT NULL,
  ncm CHAR(8) NOT NULL, cest CHAR(7), familia_produto TEXT NOT NULL,
  unidade_comercial TEXT NOT NULL,
  PRIMARY KEY (cnpj_emitente, codigo_produto)
);
CREATE TABLE IF NOT EXISTS nfe_cab (
  chave_acesso CHAR(44) PRIMARY KEY, numero_nfe INTEGER NOT NULL, serie INTEGER NOT NULL,
  dh_emissao TIMESTAMPTZ NOT NULL, cnpj_emitente CHAR(14) NOT NULL,
  cnpj_destinatario CHAR(14) NOT NULL, uf_emitente CHAR(2) NOT NULL,
  uf_destinatario CHAR(2) NOT NULL, valor_total NUMERIC(18,2) NOT NULL,
  status_nfe TEXT NOT NULL, updated_at_source TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS nfe_item (
  chave_acesso CHAR(44) NOT NULL, num_item INTEGER NOT NULL, payload JSONB NOT NULL,
  PRIMARY KEY (chave_acesso, num_item)
);
CREATE TABLE IF NOT EXISTS cdc_changelog (
  op_seq BIGINT PRIMARY KEY, table_name TEXT NOT NULL, pk TEXT NOT NULL, op CHAR(1) NOT NULL,
  op_ts TIMESTAMPTZ NOT NULL, payload_json JSONB NOT NULL
);
"""


def _connection_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "lakehouse_fiscal")
    user = os.environ.get("POSTGRES_USER", "lakehouse")
    password = get_secret("lakehouse-fiscal", "POSTGRES_PASSWORD")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _json(row: dict[str, Any]) -> str:
    return json.dumps(row, default=str, sort_keys=True)


def _chunks(rows: Iterable[tuple[Any, ...]], size: int = 2_000) -> Iterable[list[tuple[Any, ...]]]:
    chunk: list[tuple[Any, ...]] = []
    for row in rows:
        chunk.append(row)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def seed(seed_value: int = 42, scale: float = 1.0) -> dict[str, int]:
    dataset = generate(seed_value, scale)
    with psycopg.connect(_connection_url()) as connection, connection.cursor() as cursor:
        cursor.execute(DDL)
        cursor.execute("TRUNCATE cdc_changelog, nfe_item, nfe_cab, produto, destinatario, emitente")
        cursor.executemany(
            "INSERT INTO emitente VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (tuple(row.values()) for row in dataset.emitentes),
        )
        cursor.executemany(
            "INSERT INTO produto VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (tuple(row.values()) for row in dataset.produtos),
        )
        recipients = sorted({str(row["cnpj_destinatario"]) for row in dataset.nfe_cab})
        cursor.executemany(
            "INSERT INTO destinatario VALUES (%s)", ((value,) for value in recipients)
        )
        cursor.executemany(
            "INSERT INTO nfe_cab VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (tuple(row.values()) for row in dataset.nfe_cab),
        )
        for chunk in _chunks(
            (row["chave_acesso"], row["num_item"], _json(row)) for row in dataset.nfe_item
        ):
            cursor.executemany("INSERT INTO nfe_item VALUES (%s,%s,%s::jsonb)", chunk)
        issuer_cdc = [
            (
                index,
                "emitente",
                row["cnpj_emitente"],
                "I",
                dataset.nfe_cab[0]["dh_emissao"],
                _json(row),
            )
            for index, row in enumerate(dataset.emitentes, 1)
        ]
        event_cdc = [
            (
                len(issuer_cdc) + index,
                "nfe_evento",
                row["chave_acesso"],
                "I",
                row["dh_evento"],
                _json(row),
            )
            for index, row in enumerate(dataset.eventos, 1)
        ]
        cdc_rows = issuer_cdc + event_cdc
        cursor.executemany("INSERT INTO cdc_changelog VALUES (%s,%s,%s,%s,%s,%s::jsonb)", cdc_rows)
    return {
        "emitentes": dataset.volume.issuers,
        "produtos": dataset.volume.products,
        "notas": dataset.volume.invoices,
        "itens": dataset.volume.items,
        "eventos": dataset.volume.events,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scale", type=float, default=1.0)
    args = parser.parse_args()
    log_json(get_logger(__name__), "postgres_seeded", **seed(args.seed, args.scale))


if __name__ == "__main__":
    main()
