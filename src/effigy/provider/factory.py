from typing import Literal
from dataclasses import dataclass

from .base import DatabaseProvider
from .memory import InMemoryProvider

ProviderType = Literal["inmemory", "postgres", "mysql"]


def create_provider(provider_type: ProviderType, **config) -> DatabaseProvider:
    providers: dict[ProviderType, type[DatabaseProvider]] = {
        "inmemory": InMemoryProvider,
    }

    if provider_type not in providers:
        raise ValueError(f"Unknown provider type: {provider_type}")

    provider_cls = providers[provider_type]
    if "from_env" in config:
        ...  # TODO: load from environment

    if "from_url" in config:
        ...  # TODO: load from URL

    return provider_cls(**config)
