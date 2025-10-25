from typing import Protocol, Any
from dataclasses import dataclass, field


class DatabaseProvider(Protocol):
    def get_connection_string(self) -> str:
        """Gets the complete SQLAlchemy compatible connection string for the database.

        Format: `dialect+driver://username:password@host:port/database`

        Returns:
            A database connection string
        """
        ...

    # TODO: probably want to come up with a type instead of dict[str, Any]
    def get_engine_options(self) -> dict[str, Any]:
        """Gets SQLAlchemy engine configuration options."""
        ...


@dataclass(frozen=True)
class BaseEngineOptions:

    pool_size: int = 5
    echo: bool = False
    echo_pool: bool = False
    max_overflow: int = 10
    pool_timeout: float = 60.0
    pool_recycle: float = 3600
    pool_preping: bool = False
    isolation_level: str | None = None
    connect_args: dict[str, Any] = field(default_factory=dict)

    def to_engine_opts(self) -> dict[str, Any]:
        """Converts this engine options instance into SQLAlchemy engine keyword arguments"""

        opts: dict[str, Any] = {
            "pool_size": self.pool_size,
            "echo": self.echo,
            "echo_pool": self.echo_pool,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "pool_preping": self.pool_preping,
        }
        if self.isolation_level is not None:
            opts["isolation_level"] = self.isolation_level
        if self.connect_args:
            opts["connect_args"] = self.connect_args
        return opts
