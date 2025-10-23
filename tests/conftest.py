import pytest

from effigy.entity import entity
from effigy.provider.memory import InMemoryProvider


@pytest.fixture
def in_memory_provider() -> InMemoryProvider:
    """Provides a synchronous in-memory database provider for testing"""
    return InMemoryProvider(use_async=False)


@pytest.fixture
def async_in_memory_provider() -> InMemoryProvider:
    """Provides an asynchronous in-memory database provider for testing"""
    return InMemoryProvider(use_async=True)


@pytest.fixture
def sample_user_entity() -> type:
    """Provides a sample User entity class for testing

    Returns:
        An @entity decorated User class with id, name, and email fields
    """

    @entity
    class User:
        id: int
        name: str
        email: str

    return User


@pytest.fixture
def sample_post_entity() -> type:
    """Provides a sample Post entity class for testing relationships

    Returns:
        An @entity decorated Post class with id, title, content, and user_id fields
    """

    @entity
    class Post:
        id: int
        title: str
        content: str
        user_id: int

    return Post


@pytest.fixture
def sample_entities(sample_user_entity: type, sample_post_entity: type) -> dict[str, type]:
    return {"User": sample_user_entity, "Post": sample_post_entity}
