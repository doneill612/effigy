from typing import Any

from pydantic import Field
from sqlalchemy.pool import StaticPool

from .base import BaseEngineOptions, DatabaseProvider


class InMemoryEngineOptions(BaseEngineOptions):

    use_async: bool = Field(default=False)

    def to_engine_opts(self) -> dict[str, Any]:
        """Converts this engine options instance into SQLAlchemy engine keyword arguments.

        Note: StaticPool doesn't accept pool_* parameters, so we only include
        echo, connect_args, and isolation_level.
        """
        opts: dict[str, Any] = {
            "poolclass": StaticPool,
            "echo": self.echo,
            "echo_pool": self.echo_pool,
        }

        # For sync mode, we need check_same_thread=False for SQLite
        if not self.use_async:
            connect_args = self.connect_args.copy() if self.connect_args else {}
            connect_args["check_same_thread"] = False
            opts["connect_args"] = connect_args
        elif self.connect_args:
            opts["connect_args"] = self.connect_args.copy()

        if self.isolation_level is not None:
            opts["isolation_level"] = self.isolation_level

        return opts


class InMemoryProvider(DatabaseProvider[InMemoryEngineOptions]):
    """A simple in-memory database provider that leverages sqlite"""

    def __init__(self, options: InMemoryEngineOptions):
        self._opt = options

    def get_connection_string(self) -> str:
        if self._opt.use_async:
            return "sqlite+aiosqlite:///:memory:"
        return "sqlite:///:memory:"

    def get_engine_options(self) -> dict[str, Any]:
        return self._opt.to_engine_opts()
