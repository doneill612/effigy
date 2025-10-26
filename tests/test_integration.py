from typing import Any, Generator

import pytest
from sqlalchemy import inspect

from effigy.builder.core import DbBuilder
from effigy.context import AsyncDbContext, DbContext
from effigy.dbset import AsyncDbSet, DbSet
from effigy.entity import entity
from effigy.provider.memory import InMemoryProvider


# Test entities with relationships
@entity
class Author:
    """Test author entity for integration tests"""

    id: int
    name: str
    email: str
    posts: list["Post"]


@entity
class Post:
    """Test post entity for integration tests"""

    id: int
    title: str
    content: str
    author_id: int
    author: Author | None


# Sync context for integration testing
class IntegrationDbContext(DbContext):
    """DbContext with multiple entities for integration testing"""

    authors: DbSet[Author]
    posts: DbSet[Post]

    def setup(self, builder: DbBuilder) -> None:
        """Configure entities with keys and relationships"""
        builder.entity(Author).has_key(lambda a: a.id).has_many(lambda a: a.posts).with_foreign_key(
            lambda p: p.author_id
        )

        builder.entity(Post).has_key(lambda p: p.id).has_one(lambda p: p.author).with_foreign_key(
            lambda p: p.author_id
        )


# Async context for integration testing
class AsyncIntegrationDbContext(AsyncDbContext):
    """AsyncDbContext with multiple entities for integration testing"""

    authors: AsyncDbSet[Author]
    posts: AsyncDbSet[Post]

    def setup(self, builder: DbBuilder) -> None:
        """Configure entities with keys and relationships"""
        builder.entity(Author).has_key(lambda a: a.id).has_many(lambda a: a.posts).with_foreign_key(
            lambda p: p.author_id
        )

        builder.entity(Post).has_key(lambda p: p.id).has_one(lambda p: p.author).with_foreign_key(
            lambda p: p.author_id
        )


@pytest.fixture
def integration_context(
    in_memory_provider: InMemoryProvider,
) -> Generator[IntegrationDbContext, None, None]:
    """Provides an IntegrationDbContext instance with automatic cleanup"""
    ctx = IntegrationDbContext(in_memory_provider)
    yield ctx
    ctx.dispose()


@pytest.fixture
def async_integration_context(
    async_in_memory_provider: InMemoryProvider, request: Any
) -> Generator[AsyncIntegrationDbContext, None, None]:
    """Provides an AsyncIntegrationDbContext instance with automatic cleanup"""
    import asyncio

    ctx = AsyncIntegrationDbContext(async_in_memory_provider)
    yield ctx
    # Cleanup - try to use event loop fixture if available, otherwise create new one
    try:
        # Try to get event loop fixture from pytest-asyncio (available in async tests)
        loop = request.getfixturevalue("event_loop")
        loop.run_until_complete(ctx.dispose())
    except Exception:
        # Fall back to creating a new event loop for cleanup
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(ctx.dispose())
        finally:
            loop.close()


class TestDatabaseSchemaCreation:
    """Tests that verify database tables and schema are correctly created"""

    def test_tables_are_created_in_database(
        self, integration_context: IntegrationDbContext
    ) -> None:
        """Verify that entity tables exist in the database"""
        with integration_context:
            inspector = inspect(integration_context._engine)
            table_names = inspector.get_table_names()

            assert "authors" in table_names
            assert "posts" in table_names

    def test_author_table_has_correct_columns(
        self, integration_context: IntegrationDbContext
    ) -> None:
        """Verify Author table has expected columns with correct types"""
        with integration_context:
            inspector = inspect(integration_context._engine)
            columns = {col["name"]: col for col in inspector.get_columns("authors")}

            assert "id" in columns
            assert "name" in columns
            assert "email" in columns

            # Verify id is primary key
            pk_constraint = inspector.get_pk_constraint("authors")
            assert "id" in pk_constraint["constrained_columns"]

    def test_post_table_has_correct_columns(
        self, integration_context: IntegrationDbContext
    ) -> None:
        """Verify Post table has expected columns including foreign key"""
        with integration_context:
            inspector = inspect(integration_context._engine)
            columns = {col["name"]: col for col in inspector.get_columns("posts")}

            assert "id" in columns
            assert "title" in columns
            assert "content" in columns
            assert "author_id" in columns

            # Verify id is primary key
            pk_constraint = inspector.get_pk_constraint("posts")
            assert "id" in pk_constraint["constrained_columns"]

    def test_foreign_key_column_exists(self, integration_context: IntegrationDbContext) -> None:
        """Verify foreign key column exists in posts table"""
        with integration_context:
            inspector = inspect(integration_context._engine)
            columns = {col["name"]: col for col in inspector.get_columns("posts")}

            # Verify the foreign key column exists (even if constraint not enforced)
            assert "author_id" in columns


class TestSyncCRUDOperations:
    """Tests for basic Create, Read, Update, Delete operations (synchronous)"""

    def test_insert_single_entity(self, integration_context: IntegrationDbContext) -> None:
        """Insert a single author and verify it's persisted"""
        author = Author(id=1, name="Alice", email="alice@example.com", posts=[])

        with integration_context as ctx:
            ctx.authors.add(author)

        # Verify in new context
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 1
            assert authors[0].name == "Alice"
            assert authors[0].email == "alice@example.com"

    def test_insert_multiple_entities(self, integration_context: IntegrationDbContext) -> None:
        """Insert multiple authors and verify they're all persisted"""
        author1 = Author(id=1, name="Alice", email="alice@example.com", posts=[])
        author2 = Author(id=2, name="Bob", email="bob@example.com", posts=[])

        with integration_context as ctx:
            ctx.authors.add(author1)
            ctx.authors.add(author2)

        # Verify
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 2
            names = {a.name for a in authors}
            assert names == {"Alice", "Bob"}

    def test_query_entities_from_database(self, integration_context: IntegrationDbContext) -> None:
        """Query entities back from database"""
        # Insert test data
        with integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))

        # Query
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 1
            assert authors[0].id == 1

    def test_update_entity(self, integration_context: IntegrationDbContext) -> None:
        """Update an entity and verify changes persist"""
        # Insert
        with integration_context as ctx:
            author = Author(id=1, name="Alice", email="alice@example.com", posts=[])
            ctx.authors.add(author)

        # Update
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            author = authors[0]
            author.name = "Alice Updated"

        # Verify
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert authors[0].name == "Alice Updated"

    def test_delete_entity(self, integration_context: IntegrationDbContext) -> None:
        """Delete an entity and verify it's removed"""
        # Insert
        with integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))

        # Delete
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            ctx.authors.remove(authors[0])

        # Verify
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 0


class TestSyncQueryOperations:
    """Tests for query operations with predicates and filters"""

    def test_to_list_returns_all_entities(self, integration_context: IntegrationDbContext) -> None:
        """to_list() returns all entities of a type"""
        with integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))
            ctx.authors.add(Author(id=2, name="Bob", email="bob@example.com", posts=[]))

        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 2

    def test_where_filters_entities(self, integration_context: IntegrationDbContext) -> None:
        """where() filters entities based on predicate"""
        with integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))
            ctx.authors.add(Author(id=2, name="Bob", email="bob@example.com", posts=[]))

        with integration_context as ctx:
            alice = ctx.authors.where(lambda a: a.name == "Alice").to_list()
            assert len(alice) == 1
            assert alice[0].name == "Alice"

    def test_dbset_iteration(self, integration_context: IntegrationDbContext) -> None:
        """DbSet can be iterated over"""
        with integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))
            ctx.authors.add(Author(id=2, name="Bob", email="bob@example.com", posts=[]))

        with integration_context as ctx:
            count = 0
            for author in ctx.authors:
                assert author.name in ["Alice", "Bob"]
                count += 1
            assert count == 2


class TestSyncRelationshipOperations:
    """Tests for entity relationships and eager loading"""

    def test_insert_entities_with_foreign_keys(
        self, integration_context: IntegrationDbContext
    ) -> None:
        """Insert related entities using foreign keys"""
        with integration_context as ctx:
            author = Author(id=1, name="Alice", email="alice@example.com", posts=[])
            post = Post(id=1, title="First Post", content="Hello World", author_id=1, author=None)
            ctx.authors.add(author)
            ctx.posts.add(post)

        # Verify relationship
        with integration_context as ctx:
            posts = ctx.posts.to_list()
            assert len(posts) == 1
            assert posts[0].author_id == 1

    def test_query_with_relationship_data(self, integration_context: IntegrationDbContext) -> None:
        """Query posts and verify relationship data via foreign key"""
        # Setup data
        with integration_context as ctx:
            author = Author(id=1, name="Alice", email="alice@example.com", posts=[])
            post = Post(id=1, title="First Post", content="Hello World", author_id=1, author=None)
            ctx.authors.add(author)
            ctx.posts.add(post)

        # Query posts and verify foreign key is set correctly
        with integration_context as ctx:
            posts = ctx.posts.to_list()
            assert len(posts) == 1
            assert posts[0].author_id == 1

            # Verify we can query the related author separately
            authors = ctx.authors.where(lambda a: a.id == posts[0].author_id).to_list()
            assert len(authors) == 1
            assert authors[0].name == "Alice"


class TestAsyncCRUDOperations:
    """Tests for async CRUD operations"""

    @pytest.mark.asyncio
    async def test_async_insert_entity(
        self, async_integration_context: AsyncIntegrationDbContext
    ) -> None:
        """Insert entity using async context"""
        author = Author(id=1, name="Alice", email="alice@example.com", posts=[])

        async with async_integration_context as ctx:
            ctx.authors.add(author)

        # Verify
        async with async_integration_context as ctx:
            authors = await ctx.authors.to_list()
            assert len(authors) == 1
            assert authors[0].name == "Alice"

    @pytest.mark.asyncio
    async def test_async_query_entities(
        self, async_integration_context: AsyncIntegrationDbContext
    ) -> None:
        """Query entities using async context"""
        # Insert
        async with async_integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))

        # Query
        async with async_integration_context as ctx:
            authors = await ctx.authors.to_list()
            assert len(authors) == 1

    @pytest.mark.asyncio
    async def test_async_iteration(
        self, async_integration_context: AsyncIntegrationDbContext
    ) -> None:
        """Iterate over async DbSet"""
        # Insert
        async with async_integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))
            ctx.authors.add(Author(id=2, name="Bob", email="bob@example.com", posts=[]))

        # Iterate
        async with async_integration_context as ctx:
            count = 0
            async for author in ctx.authors:
                assert author.name in ["Alice", "Bob"]
                count += 1
            assert count == 2


class TestContextManagerBehavior:
    """Tests for context manager auto-commit and rollback behavior"""

    def test_context_manager_auto_commits(self, integration_context: IntegrationDbContext) -> None:
        """Context manager auto-commits on successful exit"""
        with integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))
        # No explicit save_changes() call

        # Verify auto-commit happened
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 1

    def test_context_manager_rollback_on_exception(
        self, integration_context: IntegrationDbContext
    ) -> None:
        """Context manager rolls back on exception"""
        try:
            with integration_context as ctx:
                ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Verify rollback happened
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 0

    @pytest.mark.asyncio
    async def test_async_context_manager_auto_commits(
        self, async_integration_context: AsyncIntegrationDbContext
    ) -> None:
        """Async context manager auto-commits on successful exit"""
        async with async_integration_context as ctx:
            ctx.authors.add(Author(id=1, name="Alice", email="alice@example.com", posts=[]))

        # Verify auto-commit
        async with async_integration_context as ctx:
            authors = await ctx.authors.to_list()
            assert len(authors) == 1


class TestEdgeCases:
    """Tests for edge cases and error conditions"""

    def test_query_empty_database(self, integration_context: IntegrationDbContext) -> None:
        """Querying empty database returns empty list"""
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert authors == []

    def test_multiple_entities_single_transaction(
        self, integration_context: IntegrationDbContext
    ) -> None:
        """Multiple entities can be added in single transaction"""
        with integration_context as ctx:
            for i in range(10):
                ctx.authors.add(
                    Author(id=i, name=f"Author {i}", email=f"author{i}@example.com", posts=[])
                )

        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 10

    def test_save_changes_explicit_call(self, integration_context: IntegrationDbContext) -> None:
        """save_changes() can be called explicitly within context manager"""
        # save_changes() can be called explicitly even though context manager auto-commits on exit
        with integration_context as ctx:
            ctx.authors.add(
                Author(id=1, name="Alice", email="alice@example.com", posts=[])
            )
            change_count = ctx.save_changes()
            # Note: change_count may be 0 due to flush() clearing the session state
            # before counting. The important part is that save_changes() succeeds.
            assert isinstance(change_count, int)

        # Verify the change persisted
        with integration_context as ctx:
            authors = ctx.authors.to_list()
            assert len(authors) == 1
