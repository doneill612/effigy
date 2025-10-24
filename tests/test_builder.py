from typing import Optional

import pytest
from sqlalchemy import Boolean, Float, Integer, String, inspect
from effigy.builder.core import DbBuilder
from effigy.context import DbContext
from effigy.dbset import DbSet
from effigy.entity import entity
from effigy.provider.memory import InMemoryProvider


class TestTableCreation:
    """Tests for DbBuilder._create_table() functionality"""

    def test_creates_table_with_correct_name(self) -> None:
        """Table should be created with the entity's __tablename__"""

        @entity
        class Product:
            id: int
            name: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        # verify table was created with correct name
        assert hasattr(Product, "__table__")
        table = getattr(Product, "__table__")
        assert table is not None
        assert getattr(table, "name") == "products"

    def test_creates_columns_with_correct_types(self) -> None:
        """Columns should be created with SQLAlchemy types matching Python types"""

        @entity
        class Product:
            id: int
            name: str
            price: float
            active: bool

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        assert hasattr(Product, "__table__")
        table = getattr(Product, "__table__", None)
        assert table is not None

        # verify column types
        assert isinstance(table.c.id.type, Integer)
        assert isinstance(table.c.name.type, String)
        assert isinstance(table.c.price.type, Float)
        assert isinstance(table.c.active.type, Boolean)

    def test_creates_nullable_columns_for_optional_types(self) -> None:
        """Optional types should create nullable columns"""

        @entity
        class Product:
            id: int
            name: str
            description: Optional[str]

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        assert hasattr(Product, "__table__")
        table = getattr(Product, "__table__", None)
        assert table is not None

        # id and name should not be nullable
        assert not table.c.id.nullable
        assert not table.c.name.nullable
        # description should be nullable
        assert table.c.description.nullable

    def test_creates_primary_key_columns(self) -> None:
        """Columns marked with has_key should be primary keys"""

        @entity
        class Product:
            id: int
            name: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        assert hasattr(Product, "__table__")
        table = getattr(Product, "__table__", None)
        assert table is not None

        # verify primary key
        assert table.c.id.primary_key
        assert not table.c.name.primary_key

    def test_creates_composite_primary_keys(self) -> None:
        """Multiple has_key calls should create composite primary key"""

        @entity
        class OrderLine:
            order_id: int
            product_id: int
            quantity: int

        class TestContext(DbContext):
            order_lines: DbSet[OrderLine]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(OrderLine).has_key(lambda o: o.order_id).has_key(
                    lambda o: o.product_id
                )

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        assert hasattr(OrderLine, "__table__")
        table = getattr(OrderLine, "__table__", None)
        assert table is not None

        # verify both columns are primary keys
        assert table.c.order_id.primary_key
        assert table.c.product_id.primary_key
        assert not table.c.quantity.primary_key

    def test_skips_collection_fields(self) -> None:
        """list, dict, and set fields should not create columns"""

        @entity
        class User:
            id: int
            name: str
            posts: list[str]  # should be skipped
            metadata: dict[str, str]  # should be skipped
            tags: set[str]  # should be skipped

        class TestContext(DbContext):
            users: DbSet[User]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(User).has_key(lambda u: u.id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        assert hasattr(User, "__table__")
        table = getattr(User, "__table__", None)
        assert table is not None

        # verify only scalar columns exist
        column_names = [c.name for c in table.columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "posts" not in column_names
        assert "metadata" not in column_names
        assert "tags" not in column_names

    def test_rejects_non_optional_union_types(self) -> None:
        """Union types other than Optional[T] should raise TypeError"""

        @entity
        class BadEntity:
            id: int
            value: int | str  # invalid union

        class TestContext(DbContext):
            bad_entities: DbSet[BadEntity]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(BadEntity).has_key(lambda b: b.id)

        provider = InMemoryProvider(use_async=False)

        with pytest.raises(TypeError, match="unsupported union type"):
            TestContext(provider)

    def test_requires_at_least_one_primary_key(self) -> None:
        """Entities without primary keys should raise ValueError"""

        @entity
        class NoPrimaryKey:
            name: str

        class TestContext(DbContext):
            entities: DbSet[NoPrimaryKey]

            def setup(self, builder: DbBuilder) -> None:
                # intentionally not calling has_key
                builder.entity(NoPrimaryKey)

        provider = InMemoryProvider(use_async=False)

        with pytest.raises(ValueError, match="must have at least one primary key"):
            TestContext(provider)

    def test_creates_sqlalchemy_mapper(self) -> None:
        """Entities should be registered with SQLAlchemy ORM mapper"""

        @entity
        class Product:
            id: int
            name: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        # verify mapper was created
        assert hasattr(Product, "__mapper__")
        mapper = inspect(Product)
        assert mapper is not None
        assert mapper.class_ == Product


class TestPropertyConfiguration:
    """Tests for property configuration (unique, required, default)"""

    # TODO: property configuration tests require InstrumentedAttribute
    # which only exists after SQLAlchemy mapping. Need to revisit the
    # property configuration API to support string-based configuration
    # like has_key does, or defer property config until after initial mapping

    @pytest.mark.skip(reason="Property configuration API needs string-based support")
    def test_property_unique_creates_unique_column(self) -> None:
        """property marked as unique should create unique constraint"""
        pass

    @pytest.mark.skip(reason="Property configuration API needs string-based support")
    def test_property_default_value(self) -> None:
        """property with default should set column default"""
        pass


class TestRelationshipCreation:
    """Tests for DbBuilder._create_relationships() functionality"""

    # TODO: relationship configuration tests require has_many() and has_one()
    # methods on EntityConfiguration which haven't been implemented yet.
    # The relationship configuration API and builder methods need to be
    # designed and implemented first.

    @pytest.mark.skip(reason="has_many() and has_one() API not yet implemented")
    def test_creates_one_to_many_relationship(self) -> None:
        """One-to-many relationship should be created with correct configuration"""
        pass

    @pytest.mark.skip(reason="has_many() and has_one() API not yet implemented")
    def test_creates_many_to_one_relationship(self) -> None:
        """Many-to-one relationship should be created with correct configuration"""
        pass

    @pytest.mark.skip(reason="has_many() and has_one() API not yet implemented")
    def test_bidirectional_relationship_with_backpopulates(self) -> None:
        """Bidirectional relationships should use back_populates"""
        pass


class TestBuilderFinalize:
    """Tests for DbBuilder.finalize() orchestration"""

    def test_finalize_calls_create_table_for_all_entities(self) -> None:
        """Finalize should create tables for all configured entities"""

        @entity
        class User:
            id: int
            name: str

        @entity
        class Post:
            id: int
            title: str

        class TestContext(DbContext):
            users: DbSet[User]
            posts: DbSet[Post]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(User).has_key(lambda u: u.id)
                builder.entity(Post).has_key(lambda p: p.id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        # verify both tables were created
        assert hasattr(User, "__table__")
        assert getattr(User, "__table__") is not None
        assert hasattr(Post, "__table__")
        assert getattr(Post, "__table__") is not None

    @pytest.mark.skip(reason="has_many() API not yet implemented")
    def test_finalize_calls_create_relationships_after_tables(self) -> None:
        """Finalize should create relationships after tables exist"""
        pass
