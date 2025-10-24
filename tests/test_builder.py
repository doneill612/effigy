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

    def test_property_unique_creates_unique_column(self) -> None:
        """property marked as unique should create unique constraint"""

        @entity
        class User:
            id: int
            email: str

        class TestContext(DbContext):
            users: DbSet[User]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(User).has_key(lambda u: u.id).property(lambda u: u.email).unique()

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        assert hasattr(User, "__table__")
        table = getattr(User, "__table__", None)
        assert table is not None

        # verify email column has unique constraint
        assert table.c.email.unique

    def test_property_default_value(self) -> None:
        """property with default should set column default"""

        @entity
        class Product:
            id: int
            name: str
            active: bool

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id).property(
                    lambda p: p.active
                ).with_default(True)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        assert hasattr(Product, "__table__")
        table = getattr(Product, "__table__", None)
        assert table is not None

        # verify active column has default value
        assert table.c.active.default is not None
        assert table.c.active.default.arg is True


# Module-level entities for relationship tests
@entity
class RelTestPost:
    id: int
    title: str
    user_id: int


@entity
class RelTestUser:
    id: int
    name: str
    posts: list["RelTestPost"]


@entity
class RelTestUserBidir:
    id: int
    name: str
    posts: list["RelTestPostBidir"]


@entity
class RelTestPostBidir:
    id: int
    title: str
    user_id: int
    user: RelTestUserBidir | None = None


class TestRelationshipCreation:
    """Tests for DbBuilder._create_relationships() functionality"""

    def test_creates_one_to_many_relationship(self) -> None:
        """One-to-many relationship should be created with correct configuration"""

        class TestContext(DbContext):
            users: DbSet[RelTestUser]
            posts: DbSet[RelTestPost]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(RelTestUser).has_key(lambda u: u.id).has_many(
                    lambda u: u.posts
                ).with_foreign_key(lambda p: p.user_id)
                builder.entity(RelTestPost).has_key(lambda p: p.id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        # verify relationship was created
        assert hasattr(RelTestUser, "posts")
        posts_rel = getattr(RelTestUser, "posts")
        assert posts_rel is not None

        # verify it's a SQLAlchemy relationship object
        from sqlalchemy.orm.relationships import _RelationshipDeclared

        assert isinstance(posts_rel, _RelationshipDeclared)

    def test_creates_many_to_one_relationship(self) -> None:
        """Many-to-one relationship should be created with correct configuration"""

        @entity
        class User:
            id: int
            name: str

        @entity
        class Post:
            id: int
            title: str
            user_id: int
            user: User | None = None

        class TestContext(DbContext):
            users: DbSet[User]
            posts: DbSet[Post]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(User).has_key(lambda u: u.id)
                builder.entity(Post).has_key(lambda p: p.id).has_one(
                    lambda p: p.user
                ).with_foreign_key(lambda p: p.user_id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        # verify relationship was created
        assert hasattr(Post, "user")
        user_rel = getattr(Post, "user")
        assert user_rel is not None

        # verify it's a SQLAlchemy relationship object
        from sqlalchemy.orm.relationships import _RelationshipDeclared

        assert isinstance(user_rel, _RelationshipDeclared)

    def test_bidirectional_relationship_with_backpopulates(self) -> None:
        """Bidirectional relationships should use back_populates"""

        class TestContext(DbContext):
            users: DbSet[RelTestUserBidir]
            posts: DbSet[RelTestPostBidir]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(RelTestUserBidir).has_key(lambda u: u.id).has_many(
                    lambda u: u.posts
                ).with_foreign_key(lambda p: p.user_id).backpopulates(lambda p: p.user)
                builder.entity(RelTestPostBidir).has_key(lambda p: p.id).has_one(
                    lambda p: p.user
                ).with_foreign_key(lambda p: p.user_id).backpopulates(lambda u: u.posts)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        # verify both relationships were created
        assert hasattr(RelTestUserBidir, "posts")
        assert hasattr(RelTestPostBidir, "user")

        # verify relationships are SQLAlchemy relationship objects with back_populates configured
        from sqlalchemy.orm.relationships import _RelationshipDeclared

        user_posts_rel = getattr(RelTestUserBidir, "posts")
        post_user_rel = getattr(RelTestPostBidir, "user")

        assert isinstance(user_posts_rel, _RelationshipDeclared)
        assert isinstance(post_user_rel, _RelationshipDeclared)

        # verify back_populates is configured
        assert user_posts_rel.back_populates == "user"
        assert post_user_rel.back_populates == "posts"


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

    def test_finalize_calls_create_relationships_after_tables(self) -> None:
        """Finalize should create relationships after tables exist"""

        class TestContext(DbContext):
            users: DbSet[RelTestUserBidir]
            posts: DbSet[RelTestPostBidir]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(RelTestUserBidir).has_key(lambda u: u.id).has_many(
                    lambda u: u.posts
                ).with_foreign_key(lambda p: p.user_id)
                builder.entity(RelTestPostBidir).has_key(lambda p: p.id).has_one(
                    lambda p: p.user
                ).with_foreign_key(lambda p: p.user_id)

        provider = InMemoryProvider(use_async=False)
        TestContext(provider)

        # verify tables were created
        assert hasattr(RelTestUserBidir, "__table__")
        assert hasattr(RelTestPostBidir, "__table__")

        # verify relationships were created after tables
        assert hasattr(RelTestUserBidir, "posts")
        assert hasattr(RelTestPostBidir, "user")

        # verify foreign key column exists in Post table
        post_table = getattr(RelTestPostBidir, "__table__")
        assert "user_id" in [c.name for c in post_table.columns]
