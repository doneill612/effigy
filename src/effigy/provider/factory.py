from typing import Any, Literal

from effigy.provider.mysql import MySqlProvider, MySqlEngineOptions
from effigy.provider.pg import PostgresProvider, PostgresEngineOptions

from .base import DatabaseProvider
from .memory import InMemoryProvider, InMemoryEngineOptions

ProviderType = Literal["inmemory", "postgres", "mysql"]


class ProviderFactory:
    """Factory for creating database provider instances."""

    @staticmethod
    def create_provider(
        provider_type: ProviderType,
        opt: InMemoryEngineOptions | MySqlEngineOptions | PostgresEngineOptions,
    ) -> DatabaseProvider[Any]:
        """Create a database provider based on the specified type.

        Args:
            provider_type: The type of provider to create
            opt: Engine options specific to the provider type

        Returns:
            A configured database provider instance

        Raises:
            ValueError: If an unknown provider type is specified
        """
        if provider_type == "inmemory":
            return InMemoryProvider(opt)  # type: ignore[arg-type]
        elif provider_type == "mysql":
            return MySqlProvider(opt)  # type: ignore[arg-type]
        elif provider_type == "postgres":
            return PostgresProvider(opt)  # type: ignore[arg-type]
        else:
            raise ValueError(f"Unknown provider type: {provider_type}")
