from __future__ import annotations

from typing import Final

from lakehouse_fiscal.config.profiles import Profile, Settings


class InvalidIdentifierError(ValueError):
    """Raised when a physical identifier cannot map to a logical table."""


LOGICAL_TABLES: Final[dict[str, frozenset[str]]] = {
    "bronze": frozenset(
        {
            "nfe_cab_raw",
            "nfe_item_raw",
            "nfe_xml_raw",
            "nfe_evento_raw",
            "emitente_cdc_raw",
            "_ingest_batches",
            "_cdc_watermark",
        }
    ),
    "silver": frozenset(
        {
            "nfe_header",
            "nfe_item",
            "nfe_evento",
            "emitente_current",
            "quarantine_nfe",
            "dq_metrics",
        }
    ),
    "gold": frozenset(
        {
            "dim_emitente",
            "dim_produto",
            "dim_data",
            "dim_cfop",
            "fct_nfe_item",
            "agg_faturamento_mensal_uf",
            "agg_carga_tributaria_familia",
        }
    ),
}


def resolve(layer: str, table: str, settings: Settings) -> str:
    if layer not in LOGICAL_TABLES or table not in LOGICAL_TABLES[layer]:
        raise InvalidIdentifierError(f"Unknown logical table '{layer}.{table}'.")
    if settings.profile is Profile.DATABRICKS:
        return f"{settings.catalog}.{layer}.{table}"
    return f"{settings.catalog}_{layer}.{table}"


def parse(identifier: str, settings: Settings) -> tuple[str, str, str]:
    parts = identifier.split(".")
    if settings.profile is Profile.DATABRICKS and len(parts) == 3:
        catalog, layer, table = parts
    elif settings.profile is Profile.LOCAL and len(parts) == 2 and "_" in parts[0]:
        prefix, table = parts
        marker = f"{settings.catalog}_"
        if not prefix.startswith(marker):
            raise InvalidIdentifierError(
                f"Invalid identifier '{identifier}'; expected '{settings.catalog}_<layer>.<table>'."
            )
        catalog, layer = settings.catalog, prefix.removeprefix(marker)
    else:
        shape = (
            "<catalog>.<schema>.<table>"
            if settings.profile is Profile.DATABRICKS
            else f"{settings.catalog}_<layer>.<table>"
        )
        raise InvalidIdentifierError(f"Invalid identifier '{identifier}'; expected '{shape}'.")
    if (
        catalog != settings.catalog
        or layer not in LOGICAL_TABLES
        or table not in LOGICAL_TABLES[layer]
    ):
        raise InvalidIdentifierError(
            f"Invalid identifier '{identifier}'; it is not in the logical inventory."
        )
    return catalog, layer, table
