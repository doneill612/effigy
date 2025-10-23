"""Tests for asynchronous AsyncDbContext"""

from typing import Type

import pytest

from effigy.provider.memory import InMemoryProvider
from tests.conftest import SampleAsyncDbContext


class TestAsyncDbContextInitialization:
    """Tests for AsyncDbContext initialization"""

    def test_initialization_with_provider(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """AsyncDbContext can be initialized with a provider"""
        assert async_db_context is not None
        assert hasattr(async_db_context, "_engine")
        assert hasattr(async_db_context, "_session_factory")

    def test_initialization_merges_engine_options(
        self, async_in_memory_provider: InMemoryProvider
    ) -> None:
        """AsyncDbContext initialization merges provider options with explicit options"""
        context = SampleAsyncDbContext(async_in_memory_provider, echo=True)

        assert context is not None

    def test_init_dbsets_discovers_async_dbset_attributes(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """_init_dbsets discovers and initializes AsyncDbSet attributes"""
        assert hasattr(async_db_context, "users")
        from effigy.dbset import AsyncDbSet

        assert isinstance(async_db_context.users, AsyncDbSet)


class TestAsyncDbContextConfiguration:
    """Tests for AsyncDbContext configuration API"""

    def test_configure_returns_configuration(
        self, async_db_context_class: Type[SampleAsyncDbContext]
    ) -> None:
        """configure() returns DbContextConfiguration instance"""
        from effigy.configuration import DbContextConfiguration

        config = async_db_context_class.configure()

        assert isinstance(config, DbContextConfiguration)

    def test_configure_stores_configuration(
        self, async_db_context_class: Type[SampleAsyncDbContext]
    ) -> None:
        """configure() stores configuration on class"""
        config = async_db_context_class.configure()

        assert async_db_context_class._configuration is config

    def test_create_with_configured_provider(
        self,
        async_db_context_class: Type[SampleAsyncDbContext],
        async_in_memory_provider: InMemoryProvider,
    ) -> None:
        """create() uses configured provider"""
        async_db_context_class.configure().with_provider(async_in_memory_provider)

        context = async_db_context_class.create()

        assert context is not None
        assert isinstance(context, SampleAsyncDbContext)

    def test_create_with_explicit_provider_override(
        self,
        async_db_context_class: Type[SampleAsyncDbContext],
        async_in_memory_provider: InMemoryProvider,
    ) -> None:
        """create() explicit provider overrides configured provider"""
        other_provider = InMemoryProvider(use_async=True)
        async_db_context_class.configure().with_provider(other_provider)

        context = async_db_context_class.create(provider=async_in_memory_provider)

        assert context is not None
        assert isinstance(context, SampleAsyncDbContext)

    def test_create_raises_value_error_when_no_provider(
        self, async_db_context_class: Type[SampleAsyncDbContext]
    ) -> None:
        """create() raises ValueError when no provider configured"""
        async_db_context_class._configuration = None

        with pytest.raises(ValueError, match="No database provider configured"):
            async_db_context_class.create()

    def test_create_merges_engine_options(
        self,
        async_db_context_class: Type[SampleAsyncDbContext],
        async_in_memory_provider: InMemoryProvider,
    ) -> None:
        """create() merges configured engine options with explicit options"""
        async_db_context_class.configure().with_provider(
            async_in_memory_provider
        ).with_engine_opts(echo=False)

        context = async_db_context_class.create(echo=True)

        assert context is not None
        assert isinstance(context, SampleAsyncDbContext)


class TestAsyncDbContextSession:
    """Tests for AsyncDbContext session management"""

    def test_session_raises_runtime_error_before_aenter(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """session property raises RuntimeError if accessed before async context"""
        with pytest.raises(RuntimeError, match="Session not initialized"):
            _ = async_db_context.session

    @pytest.mark.asyncio
    async def test_session_initialized_in_aenter(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """__aenter__ initializes session"""
        async with async_db_context:
            session = async_db_context.session
            assert session is not None

    @pytest.mark.asyncio
    async def test_session_reuse_within_context(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """session property returns same session within async context"""
        async with async_db_context:
            session1 = async_db_context.session
            session2 = async_db_context.session
            assert session1 is session2


class TestAsyncDbContextSaveChanges:
    """Tests for AsyncDbContext save_changes method"""

    @pytest.mark.asyncio
    async def test_save_changes_commits_successfully(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """save_changes() commits changes to database"""
        async with async_db_context:
            change_count = await async_db_context.save_changes()
            assert isinstance(change_count, int)

    @pytest.mark.asyncio
    async def test_save_changes_rollback_on_exception(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """save_changes() rolls back on exception and re-raises"""
        async with async_db_context:
            _ = async_db_context.session

            assert hasattr(async_db_context, "save_changes")


class TestAsyncDbContextContextManager:
    """Tests for AsyncDbContext async context manager protocol"""

    @pytest.mark.asyncio
    async def test_aenter_returns_self(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """__aenter__ returns the context instance"""
        async with async_db_context as ctx:
            assert ctx is async_db_context

    @pytest.mark.asyncio
    async def test_aexit_auto_commits_on_success(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """__aexit__ commits changes when no exception"""
        session_before = None

        async with async_db_context:
            session_before = async_db_context.session
            assert async_db_context.session is not None

        assert session_before is not None

    @pytest.mark.asyncio
    async def test_aexit_rollback_on_exception(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """__aexit__ rolls back on exception"""
        session_before = None

        try:
            async with async_db_context:
                session_before = async_db_context.session
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert session_before is not None


class TestAsyncDbContextDispose:
    """Tests for AsyncDbContext dispose method"""

    @pytest.mark.asyncio
    async def test_dispose_closes_session(
        self, async_db_context: SampleAsyncDbContext
    ) -> None:
        """dispose() closes the session and disposes engine"""
        async with async_db_context:
            session = async_db_context.session
            assert session is not None

        await async_db_context.dispose()

        assert async_db_context is not None

    @pytest.mark.asyncio
    async def test_dispose_handles_no_session(
        self, async_in_memory_provider: InMemoryProvider
    ) -> None:
        """dispose() works when session was never created"""
        context = SampleAsyncDbContext(async_in_memory_provider)

        await context.dispose()

        assert True
