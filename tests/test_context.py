"""Tests for synchronous DbContext"""

from typing import Type

import pytest

from effigy.provider.memory import InMemoryProvider
from tests.conftest import SampleDbContext


class TestDbContextInitialization:
    """Tests for DbContext initialization"""

    def test_initialization_with_provider(self, db_context: SampleDbContext) -> None:
        """DbContext can be initialized with a provider"""
        assert db_context is not None
        assert hasattr(db_context, "_engine")
        assert hasattr(db_context, "_session_factory")

    def test_initialization_merges_engine_options(
        self, in_memory_provider: InMemoryProvider
    ) -> None:
        """DbContext initialization merges provider options with explicit options"""
        context = SampleDbContext(in_memory_provider, echo=True)
        try:
            assert context is not None
        finally:
            context.dispose()

    def test_init_dbsets_discovers_dbset_attributes(self, db_context: SampleDbContext) -> None:
        """_init_dbsets discovers and initializes DbSet attributes"""
        assert hasattr(db_context, "users")
        from effigy.dbset import DbSet

        assert isinstance(db_context.users, DbSet)


class TestDbContextConfiguration:
    """Tests for DbContext configuration API"""

    def test_configure_returns_configuration(self, db_context_class: Type[SampleDbContext]) -> None:
        """configure() returns DbContextConfiguration instance"""
        from effigy.configuration import DbContextConfiguration

        config = db_context_class.configure()

        assert isinstance(config, DbContextConfiguration)

    def test_configure_stores_configuration(self, db_context_class: Type[SampleDbContext]) -> None:
        """configure() stores configuration on class"""
        config = db_context_class.configure()

        assert db_context_class._configuration is config

    def test_create_with_configured_provider(
        self, db_context_class: Type[SampleDbContext], in_memory_provider: InMemoryProvider
    ) -> None:
        """create() uses configured provider"""
        db_context_class.configure().with_provider(in_memory_provider)

        context = db_context_class.create()
        try:
            assert context is not None
            assert isinstance(context, SampleDbContext)
        finally:
            context.dispose()

    def test_create_with_explicit_provider_override(
        self, db_context_class: Type[SampleDbContext], in_memory_provider: InMemoryProvider
    ) -> None:
        """create() explicit provider overrides configured provider"""
        other_provider = InMemoryProvider()
        db_context_class.configure().with_provider(other_provider)

        context = db_context_class.create(provider=in_memory_provider)
        try:
            assert context is not None
            assert isinstance(context, SampleDbContext)
        finally:
            context.dispose()

    def test_create_raises_value_error_when_no_provider(
        self, db_context_class: Type[SampleDbContext]
    ) -> None:
        """create() raises ValueError when no provider configured"""
        db_context_class._configuration = None

        with pytest.raises(ValueError, match="No database provider configured"):
            db_context_class.create()

    def test_create_merges_engine_options(
        self, db_context_class: Type[SampleDbContext], in_memory_provider: InMemoryProvider
    ) -> None:
        """create() merges configured engine options with explicit options"""
        db_context_class.configure().with_provider(in_memory_provider).with_engine_opts(echo=False)

        context = db_context_class.create(echo=True)
        try:
            assert context is not None
            assert isinstance(context, SampleDbContext)
        finally:
            context.dispose()


class TestDbContextSession:
    """Tests for DbContext session management"""

    def test_session_lazy_initialization(self, db_context: SampleDbContext) -> None:
        """_get_session() creates session on first access"""
        assert db_context._session is None
        session = db_context._get_session()
        assert session is not None
        assert db_context._session is session

    def test_session_reuse_on_multiple_accesses(self, db_context: SampleDbContext) -> None:
        """_get_session() returns same session on multiple accesses"""
        session1 = db_context._get_session()
        session2 = db_context._get_session()

        assert session1 is session2


class TestDbContextSaveChanges:
    """Tests for DbContext save_changes method"""

    def test_save_changes_commits_successfully(self, db_context: SampleDbContext) -> None:
        """save_changes() commits changes to database"""
        change_count = db_context.save_changes()

        assert isinstance(change_count, int)


class TestDbContextContextManager:
    """Tests for DbContext context manager protocol"""

    def test_enter_returns_self(self, db_context: SampleDbContext) -> None:
        """__enter__ returns the context instance"""
        with db_context as ctx:
            assert ctx is db_context

    def test_exit_auto_commits_on_success(self, db_context: SampleDbContext) -> None:
        """__exit__ commits changes when no exception"""
        session_before = None

        with db_context:
            session_before = db_context._get_session()
            assert db_context._get_session() is not None

        assert session_before is not None

    def test_exit_rollback_on_exception(self, db_context: SampleDbContext) -> None:
        """__exit__ rolls back on exception"""
        session_before = None

        try:
            with db_context:
                session_before = db_context._get_session()
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert session_before is not None


class TestDbContextDispose:
    """Tests for DbContext dispose method"""

    def test_dispose_closes_session(self, db_context: SampleDbContext) -> None:
        """dispose() closes the session and disposes engine"""
        session = db_context._get_session()
        assert session is not None

        db_context.dispose()

        assert db_context is not None

    def test_dispose_handles_no_session(self, in_memory_provider: InMemoryProvider) -> None:
        """dispose() works when session was never created"""
        context = SampleDbContext(in_memory_provider)

        context.dispose()

        assert True
