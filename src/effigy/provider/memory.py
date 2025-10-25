from typing import Any

from pydantic import Field
from sqlalchemy.pool import StaticPool

from .base import BaseEngineOptions, DatabaseProvider


class InMemoryEngineOptions(BaseEngineOptions):

    use_async: bool = Field(default=False)

    def to_engine_opts(self) -> dict[str, Any]:
        opts = super().to_engine_opts()

        opts["poolclass"] = StaticPool
        return opts


class InMemoryProvider(DatabaseProvider[InMemoryEngineOptions]):
    """A simple in-memory database provider that leverages sqlite"""

    def get_connection_string(self) -> str:
        if self._opt.use_async:
            return "sqlite+aiosqlite:///:memory:"
        return "sqlite:///:memory:"
