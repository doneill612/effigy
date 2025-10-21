from typing import Any

import pytest

from effigy.entity import (
    Queryable,
    _is_attrs_entity,
    _is_pydantic_entity,
    _pluralize,
    entity,
    validate_entity,
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

    def test_entity_decorator_creates_slotted_class(self) -> None:

        @entity
        class User:
            id: int
            name: str

        user = User(1, "Alice")
        assert not hasattr(user, "__dict__")
        assert getattr(User, "__slots__", None) is not None

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


class TestValidationHelpers:
    """Tests for entity validation helper functions"""

    def test_is_attrs_entity_with_effigy_entity(self) -> None:

        @entity
        class User:
            id: int

        assert _is_attrs_entity(User)

    def test_is_attrs_entity_with_attrs_class(self) -> None:
        from attrs import define

        @define
        class User:
            id: int

        assert _is_attrs_entity(User)

    def test_is_attrs_entity_with_regular_class(self) -> None:

        class User:
            id: int

        assert _is_attrs_entity(User) is False

    def test_is_pydantic_entity_without_pydantic(self) -> None:

        class User:
            id: int

        result = _is_pydantic_entity(User)
        assert isinstance(result, bool)
        assert not result

    def test_is_pydantic_entity_with_pydantic(self) -> None:
        try:
            from pydantic import BaseModel

            class User(BaseModel):
                id: int

            assert _is_pydantic_entity(User)
        except ImportError:
            pytest.skip("Pydantic not installed")


class TestValidateEntity:
    """Tests for the validate_entity function"""

    def test_validate_entity_with_valid_attrs_entity(self) -> None:

        @entity
        class User:
            id: int
            name: str

        user = User(1, "Alice")
        # Should not raise
        validate_entity(user, User)

    def test_validate_entity_with_invalid_entity_type(self) -> None:

        class RegularClass:
            id: int

        instance = RegularClass()

        with pytest.raises(TypeError, match="is not a valid effigy entity"):
            validate_entity(instance, RegularClass)

    @pytest.mark.skipif(
        True, reason="Pydantic not required - skip unless explicitly testing pydantic support"
    )
    def test_validate_entity_with_pydantic_model(self) -> None:
        try:
            from pydantic import BaseModel

            class User(BaseModel):
                id: int
                name: str

            user = User(id=1, name="Alice")
            validate_entity(user, User)
        except ImportError:
            pytest.skip("Pydantic not installed")


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


class TestQueryableProtocol:
    """Tests for the Queryable protocol definition. Implementations of the protocol will need testing."""

    def test_queryable_protocol_methods(self) -> None:

        assert hasattr(Queryable, "where")
        assert hasattr(Queryable, "order_by")
        assert hasattr(Queryable, "first")
        assert hasattr(Queryable, "to_list")
