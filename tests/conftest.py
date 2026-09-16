from __future__ import annotations

import os
import socket
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse

import pytest


@pytest.fixture
def isolated_prefix(tmp_path: Path) -> str:
    return f"test_{tmp_path.name.replace('-', '_')}"


def _available(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.2):
            return True
    except OSError:
        return False


def pytest_runtest_setup(item: pytest.Item) -> None:
    minio = urlparse(os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000"))
    postgres_host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
    postgres_port = int(os.environ.get("POSTGRES_PORT", "5432"))
    if "integration" in item.keywords and not (
        _available(minio.hostname or "127.0.0.1", minio.port or 9000)
        and _available(postgres_host, postgres_port)
    ):
        pytest.skip("integration requires the MinIO and PostgreSQL services")


@pytest.fixture(scope="session")
def spark() -> Iterator[object]:
    from lakehouse_fiscal.config.profiles import load_settings
    from lakehouse_fiscal.session.spark_session import get_spark

    session = get_spark(load_settings())
    yield session
    session.stop()
