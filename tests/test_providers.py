import pytest

from abc import ABC, abstractmethod
from typing import Any

from effigy.provider.base import DatabaseProvider
from effigy.provider.memory import InMemoryProvider


class TestDatabaseProviderProtocol:
    """Tests for the DatabaseProvider protocol definition"""

    def test_protocol_defines_get_connection_string(self) -> None:
        """DatabaseProvider protocol requires get_connection_string method"""
        assert hasattr(DatabaseProvider, "get_connection_string")

    def test_protocol_defines_get_engine_options(self) -> None:
        """DatabaseProvider protocol requires get_engine_options method"""
        assert hasattr(DatabaseProvider, "get_engine_options")


class ProviderConfigTestBase(ABC):
    """Base class for provider configuration tests providing common test structure"""

    @abstractmethod
    def test_provider_conforms_to_protocol(self) -> None:
        """Ensures the provider implements provider protocol"""


class TestInMemoryProviderConfig(ProviderConfigTestBase):
    """Tests for InMemoryProvider configuration and behavior"""

    def test_provider_conforms_to_protocol(self) -> None:
        """InMemoryProvider implements DatabaseProvider protocol"""
        provider = InMemoryProvider()
        assert hasattr(provider, "get_engine_options")
        assert hasattr(provider, "get_connection_string")

    def test_provider_connection_string_sync(self) -> None:
        """InMemoryProvider returns correct sync SQLite connection string"""
        provider = InMemoryProvider()
        assert provider.get_connection_string() == "sqlite:///:memory:"

    def test_provider_connection_string_async(self) -> None:
        """InMemoryProvider returns correct async SQLite connection string with aiosqlite driver"""
        provider = InMemoryProvider(use_async=True)
        assert provider.get_connection_string() == "sqlite+aiosqlite:///:memory:"

    def test_provider_engine_options_sync(self) -> None:
        """InMemoryProvider sync mode returns StaticPool and check_same_thread=False"""
        opts = self._get_options(False)

        assert opts["poolclass"].__name__ == "StaticPool"
        assert "connect_args" in opts
        assert not opts["connect_args"]["check_same_thread"]

    def test_provider_engine_options_async(self) -> None:
        """InMemoryProvider async mode returns StaticPool without check_same_thread"""
        opts = self._get_options(True)

        assert opts["poolclass"].__name__ == "StaticPool"
        assert "connect_args" not in opts

    def _get_options(self, use_async: bool) -> dict[str, Any]:
        """Helper method to get engine options for testing"""
        provider = InMemoryProvider(use_async=use_async)
        return provider.get_engine_options()
