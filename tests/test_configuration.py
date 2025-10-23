import pytest

from effigy.configuration import DbContextConfiguration
from effigy.provider.memory import InMemoryProvider


class TestDbContextConfiguration:
    """Tests for DbContextConfiguration fluent configuration API"""

    def test_with_provider_sets_provider(self, in_memory_provider: InMemoryProvider) -> None:
        """with_provider() stores the provider instance"""
        config = DbContextConfiguration()
        config.with_provider(in_memory_provider)

        assert config._provider is in_memory_provider

    def test_with_provider_returns_self_fluent(self, in_memory_provider: InMemoryProvider) -> None:
        """with_provider() returns self for fluent chaining"""
        config = DbContextConfiguration()
        result = config.with_provider(in_memory_provider)

        assert result is config

    def test_with_engine_opts_updates_options(self) -> None:
        """with_engine_opts() stores engine options"""
        config = DbContextConfiguration()
        config.with_engine_opts(echo=True, pool_size=10)

        assert config._engine_opts["echo"] is True
        assert config._engine_opts["pool_size"] == 10

    def test_with_engine_opts_merges_multiple_calls(self) -> None:
        """with_engine_opts() merges options across multiple calls"""
        config = DbContextConfiguration()
        config.with_engine_opts(echo=True)
        config.with_engine_opts(pool_size=10)
        config.with_engine_opts(echo=False, max_overflow=20)

        assert config._engine_opts["echo"] is False
        assert config._engine_opts["pool_size"] == 10
        assert config._engine_opts["max_overflow"] == 20

    def test_with_engine_opts_returns_self_fluent(self) -> None:
        """with_engine_opts() returns self for fluent chaining"""
        config = DbContextConfiguration()
        result = config.with_engine_opts(echo=True)

        assert result is config

    def test_build_returns_dict_with_provider_and_engine_opts(
        self, in_memory_provider: InMemoryProvider
    ) -> None:
        """build() returns dictionary containing provider and engine_opts"""
        config = DbContextConfiguration()
        config.with_provider(in_memory_provider)
        config.with_engine_opts(echo=True, pool_size=5)

        result = config.build()

        assert isinstance(result, dict)
        assert "provider" in result
        assert "engine_opts" in result
        assert result["provider"] is in_memory_provider
        assert result["engine_opts"]["echo"] is True
        assert result["engine_opts"]["pool_size"] == 5

    def test_build_raises_value_error_when_no_provider_set(self) -> None:
        """build() raises ValueError when provider not configured"""
        config = DbContextConfiguration()
        config.with_engine_opts(echo=True)

        with pytest.raises(ValueError, match="doesn't have specify a provider"):
            config.build()

    def test_initialization_creates_empty_configuration(self) -> None:
        """DbContextConfiguration initializes with empty provider and engine_opts"""
        config = DbContextConfiguration()

        assert config._provider is None
        assert config._engine_opts == {}

    def test_fluent_chaining_full_workflow(self, in_memory_provider: InMemoryProvider) -> None:
        """Full fluent chaining workflow works end-to-end"""
        result = (
            DbContextConfiguration()
            .with_provider(in_memory_provider)
            .with_engine_opts(echo=True)
            .with_engine_opts(pool_size=10)
            .build()
        )

        assert result["provider"] is in_memory_provider
        assert result["engine_opts"]["echo"] is True
        assert result["engine_opts"]["pool_size"] == 10

    def test_build_returns_copy_of_engine_opts(self, in_memory_provider: InMemoryProvider) -> None:
        """build() returns engine_opts that can be modified without affecting config"""
        config = DbContextConfiguration()
        config.with_provider(in_memory_provider)
        config.with_engine_opts(echo=True)

        result1 = config.build()
        result1["engine_opts"]["echo"] = False

        result2 = config.build()
        assert result2["engine_opts"]["echo"] is True

    def test_with_provider_accepts_any_database_provider(self) -> None:
        """with_provider() accepts any object implementing DatabaseProvider protocol"""

        class CustomProvider:
            def get_connection_string(self) -> str:
                return "postgresql://localhost/test"

            def get_engine_options(self) -> dict[str, int]:
                return {"pool_size": 20}

        custom_provider = CustomProvider()
        config = DbContextConfiguration()
        config.with_provider(custom_provider)

        result = config.build()
        assert result["provider"] is custom_provider

    def test_with_engine_opts_accepts_arbitrary_keywords(self) -> None:
        """with_engine_opts() accepts any keyword arguments for flexibility"""
        config = DbContextConfiguration()
        config.with_engine_opts(
            custom_option="value",
            another_option=123,
            nested_dict={"key": "value"},
        )

        assert config._engine_opts["custom_option"] == "value"
        assert config._engine_opts["another_option"] == 123
        assert config._engine_opts["nested_dict"]["key"] == "value"
