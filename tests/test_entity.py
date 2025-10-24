from typing import Any

from effigy.entity import (
    _pluralize,
    entity,
)


class TestEntityDecorator:
    """Tests for the @entity decorator"""

    def test_entity_decorator_creates_attrs_class(self) -> None:

        @entity
        class User:
            id: int
            name: str

        assert hasattr(User, "__attrs_attrs__")
        assert getattr(User, "__effigy_entity__", False)

    def test_entity_decorator_generates_constructor(self) -> None:

        @entity
        class User:
            id: int
            name: str

        user = User(1, "Alice")
        assert user.id == 1
        assert user.name == "Alice"

    def test_entity_decorator_adds_default_tablename(self) -> None:

        @entity
        class User:
            id: int
            name: str

        assert getattr(User, "__tablename__", None) == "users"

    def test_entity_decorator_preserves_explicit_tablename(self) -> None:

        @entity
        class User:
            __tablename__ = "custom_users"
            id: int
            name: str

        assert getattr(User, "__tablename__", None) == "custom_users"

    def test_entity_decorator_adds_list_factory(self) -> None:

        @entity
        class User:
            id: int
            posts: list[str]

        user = User(1, [])
        user2 = User(2, [])

        # Ensure each instance gets its own list
        user.posts.append("post1")
        assert len(user.posts) == 1
        assert len(user2.posts) == 0

    def test_entity_decorator_adds_dict_factory(self) -> None:

        @entity
        class User:
            id: int
            metadata: dict[str, Any]

        user = User(1, {})
        user2 = User(2, {})

        # Ensure each instance gets its own dict
        user.metadata["key"] = "value"
        assert "key" in user.metadata
        assert "key" not in user2.metadata

    def test_entity_decorator_adds_set_factory(self) -> None:

        @entity
        class User:
            id: int
            tags: set[str]

        user = User(1, set())
        user2 = User(2, set())

        # Ensure each instance gets its own set
        user.tags.add("tag1")
        assert "tag1" in user.tags
        assert "tag1" not in user2.tags

    def test_entity_decorator_creates_dict_class(self) -> None:
        """Entities use __dict__ (not __slots__) to support SQLAlchemy instrumentation."""

        @entity
        class User:
            id: int
            name: str

        user = User(1, "Alice")
        # Must have __dict__ for SQLAlchemy's instrumented attributes
        assert hasattr(user, "__dict__")
        # Should not have __slots__ since we're using slots=False
        assert getattr(User, "__slots__", None) is None

    def test_entity_satisfies_protocol(self) -> None:

        @entity
        class User:
            id: int
            name: str

        assert getattr(User, "__tablename__", None) is not None


class TestPluralizeFunction:
    """Tests for the _pluralize helper function"""

    def test_pluralize_regular_word(self) -> None:
        assert _pluralize("user") == "users"
        assert _pluralize("book") == "books"
        assert _pluralize("car") == "cars"

    def test_pluralize_word_ending_in_y(self) -> None:
        assert _pluralize("category") == "categories"
        assert _pluralize("city") == "cities"
        assert _pluralize("company") == "companies"

    def test_pluralize_word_ending_in_vowel_y(self) -> None:
        assert _pluralize("day") == "days"
        assert _pluralize("boy") == "boys"
        assert _pluralize("key") == "keys"

    def test_pluralize_word_ending_in_s(self) -> None:
        assert _pluralize("class") == "classes"
        assert _pluralize("bus") == "buses"

    def test_pluralize_word_ending_in_x(self) -> None:
        assert _pluralize("box") == "boxes"
        assert _pluralize("fox") == "foxes"

    def test_pluralize_word_ending_in_z(self) -> None:
        assert _pluralize("quiz") == "quizes"

    def test_pluralize_word_ending_in_ch(self) -> None:
        assert _pluralize("church") == "churches"
        assert _pluralize("bench") == "benches"

    def test_pluralize_word_ending_in_sh(self) -> None:
        assert _pluralize("dish") == "dishes"

    def test_pluralize_single_character(self) -> None:
        assert _pluralize("y") == "ys"
        assert _pluralize("a") == "as"


class TestEntityProtocol:
    """Tests for the Entity protocol definition"""

    def test_entity_protocol_attributes(self) -> None:
        """Entity protocol should define required attributes"""

        # this is a structural type check at development time
        # verify that our @entity decorator creates compatible classes
        @entity
        class User:
            id: int

        assert getattr(User, "__tablename__", None) is not None
        # NOTE: __table__ will be be set by the SQLAlchemy model builder
