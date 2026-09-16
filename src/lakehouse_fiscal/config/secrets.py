from __future__ import annotations

import os
from typing import Protocol

from dotenv import load_dotenv

from lakehouse_fiscal.config.profiles import Profile, load_settings


class SecretNotFoundError(LookupError):
    """Raised without ever retaining or displaying a resolved secret value."""


class _SecretsApi(Protocol):
    def get(self, scope: str, key: str) -> str: ...


class _DbUtils(Protocol):
    secrets: _SecretsApi


def get_secret(scope: str, key: str, *, dbutils: _DbUtils | None = None) -> str:
    load_dotenv()
    settings = load_settings()
    value: str | None = None
    if settings.profile is Profile.LOCAL:
        value = os.environ.get(key)
    elif dbutils is not None:
        api = dbutils.secrets
        value = str(api.get(scope=scope, key=key))
    if not value:
        raise SecretNotFoundError(
            f"Secret not found: scope='{scope}', key='{key}'. For profile 'local', add it to "
            f".env; for 'databricks', run: databricks secrets put-secret {scope} {key}"
        )
    return value
