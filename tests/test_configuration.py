import pytest

from abc import ABC, abstractmethod
from typing import Any

from effigy.provider.base import DatabaseProvider
from effigy.provider.memory import InMemoryProvider


class ProviderConfigTestBase(ABC):
    @abstractmethod
    def test_provider_conforms_to_protocol(self):
        """Ensures the provider implements provider protocol"""
        ...


class TestInMemoryProviderConfig(ProviderConfigTestBase):

    def test_provider_conforms_to_protocol(self):
        provider = InMemoryProvider()
        assert hasattr(provider, "get_engine_options")
        provider = InMemoryProvider()
        assert hasattr(provider, "get_connection_string")

    def test_provider_connection_string_sync(self):
        provider = InMemoryProvider()
        assert provider.get_connection_string() == "sqlite:///:memory:"

    def test_provider_connection_string_async(self):
        provider = InMemoryProvider(use_async=True)
        assert provider.get_connection_string() == "sqlite+aiosqlite:///:memory:"

    def _get_options(self, use_async: bool) -> dict[str, Any]:
        provider = InMemoryProvider(use_async=use_async)
        return provider.get_engine_options()

    def test_provider_engine_options_sync(self):
        opts = self._get_options(False)

        assert opts["poolclass"].__name__ == "StaticPool"
        assert "connect_args" in opts
        assert not opts["connect_args"]["check_same_thread"]

    def test_provider_engine_options_async(self):
        opts = self._get_options(True)

        assert opts["poolclass"].__name__ == "StaticPool"
        assert "connect_args" not in opts
