from typing import Any

from sqlalchemy.pool import StaticPool

from .base import BaseEngineOptions


class InMemoryProvider:
    """A simple in-memory database provider that leverages sqlite"""

    def __init__(self, use_async: bool = False):
        self._async = use_async

        connect_args = {} if use_async else {"check_same_thread": False}
        self._options = BaseEngineOptions(pool_size=1, max_overflow=0, connect_args=connect_args)

    def get_connection_string(self) -> str:
        if self._async:
            return "sqlite+aiosqlite:///:memory:"
        return "sqlite:///:memory:"

    def get_engine_options(self) -> dict[str, Any]:
        opts = self._options.to_engine_opts()
        opts["poolclass"] = StaticPool
        return opts
