from typing import Protocol, Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field


T = TypeVar("T", bound="BaseEngineOptions", covariant=True)

class DatabaseProvider(Protocol[T]):
    _opt: T

    def __init__(self, options: T):
        self._opt = options

    def get_connection_string(self) -> str:
        """Gets the complete SQLAlchemy compatible connection string for the database.

        Format: `dialect+driver://username:password@host:port/database`

        Returns:
            A database connection string
        """
        ...

    def get_engine_options(self) -> dict[str, Any]:
        """Gets SQLAlchemy engine configuration options."""
        return self._opt.to_engine_opts()


class BaseEngineOptions(BaseModel):

    model_config = ConfigDict(frozen=True)

    pool_size: int = Field(default=5)
    echo: bool = Field(default=False)
    echo_pool: bool = Field(default=False)
    max_overflow: int = Field(default=10)
    pool_timeout: float = Field(default=60.0)
    pool_recycle: float = Field(default=3600)
    pool_preping: bool = Field(default=False)
    isolation_level: str | None = Field(default=None)
    connect_args: dict[str, Any] = Field(default_factory=dict)

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
