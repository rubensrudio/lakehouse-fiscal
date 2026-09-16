from __future__ import annotations

import json
import logging
import logging.config
from pathlib import Path
from typing import Any

import yaml


def _configure() -> None:
    path = Path(__file__).resolve().parents[3] / "conf" / "logging.yml"
    if path.exists():
        logging.config.dictConfig(yaml.safe_load(path.read_text(encoding="utf-8")))


def get_logger(name: str) -> logging.Logger:
    _configure()
    return logging.getLogger(name)


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "[REDACTED]"
            if any(word in key.lower() for word in ("password", "token", "secret"))
            else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def log_json(logger: logging.Logger, event: str, **fields: Any) -> None:
    logger.info(json.dumps(_redact({"event": event, **fields}), sort_keys=True, default=str))


def log_stage_summary(
    *,
    stage: str,
    batch_id: str,
    profile: str,
    table: str,
    rows_in: int,
    rows_written: int,
    rows_quarantined: int,
    duration_ms: int,
    status: str,
    dq_pass_rate: float = 100.0,
) -> None:
    logger = get_logger("lakehouse_fiscal.pipeline")
    fields = {
        "batch_id": batch_id,
        "run_id": batch_id,
        "stage": stage,
        "profile": profile,
        "table": table,
        "rows_in": rows_in,
        "rows_written": rows_written,
        "rows_quarantined": rows_quarantined,
        "duration_ms": duration_ms,
        "status": status,
    }
    log_json(logger, "stage_summary", **fields)
    logger.info(
        "[%s] batch_id=%s profile=%s rows_in=%d rows_written=%d "
        "quarantined=%d duration=%.3fs dq_pass_rate=%.2f%%",
        stage,
        batch_id,
        profile,
        rows_in,
        rows_written,
        rows_quarantined,
        duration_ms / 1000,
        dq_pass_rate,
    )
