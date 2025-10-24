"""Shared test fixtures for effigy test suite"""

from typing import Type

import pytest

from effigy.builder.core import DbBuilder
from effigy.context import AsyncDbContext, DbContext
from effigy.dbset import AsyncDbSet, DbSet
from effigy.entity import entity
from effigy.provider.memory import InMemoryProvider


@entity
class TestUser:
    """Test user entity with basic fields"""

    id: int
    name: str
    email: str


@entity
class TestPost:
    """Test post entity for relationship testing"""

    id: int
    title: str
    content: str
    user_id: int


class SampleDbContext(DbContext):
    """Concrete synchronous DbContext for testing"""

    users: DbSet[TestUser]

    def setup(self, builder: DbBuilder) -> None:
        """Configure entities with primary keys"""
        builder.entity(TestUser).has_key(lambda u: u.id)


class SampleAsyncDbContext(AsyncDbContext):
    """Concrete asynchronous AsyncDbContext for testing"""

    users: AsyncDbSet[TestUser]

    def setup(self, builder: DbBuilder) -> None:
        """Configure entities with primary keys"""
        builder.entity(TestUser).has_key(lambda u: u.id)


@pytest.fixture
def in_memory_provider() -> InMemoryProvider:
    """Provides a synchronous in-memory database provider for testing"""
    return InMemoryProvider(use_async=False)


@pytest.fixture
def async_in_memory_provider() -> InMemoryProvider:
    """Provides an asynchronous in-memory database provider for testing"""
    return InMemoryProvider(use_async=True)


# maintain these for backward compat
@pytest.fixture
def sample_user_entity() -> Type[TestUser]:
    """Provides the TestUser entity class"""
    return TestUser


@pytest.fixture
def sample_post_entity() -> Type[TestPost]:
    """Provides the TestPost entity class"""
    return TestPost


@pytest.fixture
def sample_entities() -> dict[str, Type]:
    """Provides both sample User and Post entities as a dictionary"""
    return {"User": TestUser, "Post": TestPost}


@pytest.fixture
def db_context(in_memory_provider: InMemoryProvider) -> SampleDbContext:
    """Provides an initialized synchronous DbContext instance"""

    return SampleDbContext(in_memory_provider)


@pytest.fixture
def async_db_context(async_in_memory_provider: InMemoryProvider) -> SampleAsyncDbContext:
    """Provides an initialized asynchronous AsyncDbContext instance"""
    return SampleAsyncDbContext(async_in_memory_provider)


@pytest.fixture
def db_context_class() -> Type[SampleDbContext]:
    """Provides the SampleDbContext class for class-level operations"""
    return SampleDbContext


@pytest.fixture
def async_db_context_class() -> Type[SampleAsyncDbContext]:
    """Provides the SampleAsyncDbContext class for class-level operations"""
    return SampleAsyncDbContext


@pytest.fixture
def concrete_db_context_class() -> Type[SampleDbContext]:
    # legacy fixture
    return SampleDbContext


@pytest.fixture
def async_concrete_db_context_class() -> Type[SampleAsyncDbContext]:
    # legacy fixture
    return SampleAsyncDbContext
