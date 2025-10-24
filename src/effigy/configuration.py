from typing import Any

from .provider.base import DatabaseProvider


class DbContextConfiguration:
    """Manages configuration for DbContext instances"""

    def __init__(self) -> None:
        self._provider: DatabaseProvider | None = None
        self._engine_opts: dict[str, Any] = {}

    def with_provider(self, provider: DatabaseProvider) -> "DbContextConfiguration":
        self._provider = provider
        return self

    def with_engine_opts(self, **engine_opts: dict[str, Any]) -> "DbContextConfiguration":
        self._engine_opts.update(engine_opts)
        return self

    def build(self) -> dict[str, Any]:
        if not self._provider:
            raise ValueError("Your configuration doesn't have specify a provider")
        return {"provider": self._provider, "engine_opts": self._engine_opts.copy()}
