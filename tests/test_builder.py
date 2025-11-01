from typing import Optional

import pytest
from sqlalchemy import Boolean, Float, Integer, String, inspect
from effigy.builder.core import DbBuilder
from effigy.context import DbContext
from effigy.dbset import DbSet
from effigy.entity import entity
from effigy.provider.memory import InMemoryProvider, InMemoryEngineOptions


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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            # verify table was created with correct name
            assert hasattr(Product, "__table__")
            table = getattr(Product, "__table__")
            assert table is not None
            assert getattr(table, "name") == "products"
        finally:
            ctx.dispose()

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            assert hasattr(Product, "__table__")
            table = getattr(Product, "__table__", None)
            assert table is not None

            # verify column types
            assert isinstance(table.c.id.type, Integer)
            assert isinstance(table.c.name.type, String)
            assert isinstance(table.c.price.type, Float)
            assert isinstance(table.c.active.type, Boolean)
        finally:
            ctx.dispose()

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            assert hasattr(Product, "__table__")
            table = getattr(Product, "__table__", None)
            assert table is not None

            # id and name should not be nullable
            assert not table.c.id.nullable
            assert not table.c.name.nullable
            # description should be nullable
            assert table.c.description.nullable
        finally:
            ctx.dispose()

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            assert hasattr(Product, "__table__")
            table = getattr(Product, "__table__", None)
            assert table is not None

            # verify primary key
            assert table.c.id.primary_key
            assert not table.c.name.primary_key
        finally:
            ctx.dispose()

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
                builder.entity(OrderLine).has_key(lambda o: o.order_id, lambda o: o.product_id)

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            assert hasattr(OrderLine, "__table__")
            table = getattr(OrderLine, "__table__", None)
            assert table is not None

            # verify both columns are primary keys
            assert table.c.order_id.primary_key
            assert table.c.product_id.primary_key
            assert not table.c.quantity.primary_key
        finally:
            ctx.dispose()

    def test_autoincrement_fails_on_composite_pk(self) -> None:
        """Should not be able to create a composite primary key with autoincrement"""

        @entity
        class OrderLine:
            order_id: int
            product_id: int
            quantity: int

        class TestContext(DbContext):
            order_lines: DbSet[OrderLine]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(OrderLine).has_key(
                    lambda o: o.order_id, lambda o: o.product_id, autoincrement=True
                )

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        with pytest.raises(
            ValueError, match="Autoincrement only supported on single-column primary keys"
        ):
            TestContext(provider)

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
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
        finally:
            ctx.dispose()

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            # verify mapper was created
            assert hasattr(Product, "__mapper__")
            mapper = inspect(Product)
            assert mapper is not None
            assert mapper.class_ == Product
        finally:
            ctx.dispose()


class TestPropertyConfiguration:
    """Tests for property configuration (unique, required, default)"""

    def test_autoincrement_validation_rejects_non_optional(self) -> None:
        """property marked as autoincrementing should be rejected if not defined as an optional"""

        @entity
        class User:
            id: int
            email: str

        class TestContext(DbContext):
            users: DbSet[User]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(User).has_key(lambda u: u.id, autoincrement=True)

        provider = InMemoryProvider(InMemoryEngineOptions())
        with pytest.raises(
            TypeError,
            match="Database-generated values require an optional type. Autoincrementing field 'id' must be typed as int | None",
        ):
            TestContext(provider)

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            assert hasattr(User, "__table__")
            table = getattr(User, "__table__", None)
            assert table is not None

            # verify email column has unique constraint
            assert table.c.email.unique
        finally:
            ctx.dispose()

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            assert hasattr(Product, "__table__")
            table = getattr(Product, "__table__", None)
            assert table is not None

            # verify active column has default value
            assert table.c.active.default is not None
            assert table.c.active.default.arg is True
        finally:
            ctx.dispose()


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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            # verify relationship was created
            assert hasattr(RelTestUser, "posts")
            posts_rel = getattr(RelTestUser, "posts")
            assert posts_rel is not None

            # verify it's a SQLAlchemy relationship object
            from sqlalchemy.orm.relationships import _RelationshipDeclared

            assert isinstance(posts_rel, _RelationshipDeclared)
        finally:
            ctx.dispose()

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            # verify relationship was created
            assert hasattr(Post, "user")
            user_rel = getattr(Post, "user")
            assert user_rel is not None

            # verify it's a SQLAlchemy relationship object
            from sqlalchemy.orm.relationships import _RelationshipDeclared

            assert isinstance(user_rel, _RelationshipDeclared)
        finally:
            ctx.dispose()

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
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
        finally:
            ctx.dispose()


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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            # verify both tables were created
            assert hasattr(User, "__table__")
            assert getattr(User, "__table__") is not None
            assert hasattr(Post, "__table__")
            assert getattr(Post, "__table__") is not None
        finally:
            ctx.dispose()

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

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            # verify tables were created
            assert hasattr(RelTestUserBidir, "__table__")
            assert hasattr(RelTestPostBidir, "__table__")

            # verify relationships were created after tables
            assert hasattr(RelTestUserBidir, "posts")
            assert hasattr(RelTestPostBidir, "user")

            # verify foreign key column exists in Post table
            post_table = getattr(RelTestPostBidir, "__table__")
            assert "user_id" in [c.name for c in post_table.columns]
        finally:
            ctx.dispose()


class TestIndexes:
    """Tests for DbBuilder index functionality"""

    def test_creates_single_column_index(self) -> None:
        """Single-column index should be created on the table"""

        @entity
        class Product:
            id: int
            name: str
            sku: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id).has_index(lambda p: p.sku)

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            table = getattr(Product, "__table__")
            # Check that an index was created
            indexes = list(table.indexes)
            assert len(indexes) == 1
            assert indexes[0].name == "ix_products_sku"
            assert not indexes[0].unique
            # Check that the index is on the correct column
            assert len(indexes[0].columns) == 1
            assert "sku" in [col.name for col in indexes[0].columns]
        finally:
            ctx.dispose()

    def test_creates_composite_index(self) -> None:
        """Multi-column index should be created on the table"""

        @entity
        class Product:
            id: int
            category: str
            subcategory: str
            name: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id).has_index(
                    lambda p: p.category, lambda p: p.subcategory
                )

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            table = getattr(Product, "__table__")
            indexes = list(table.indexes)
            assert len(indexes) == 1
            assert indexes[0].name == "ix_products_category_subcategory"
            # Check that the index is on both columns
            assert len(indexes[0].columns) == 2
            column_names = [col.name for col in indexes[0].columns]
            assert "category" in column_names
            assert "subcategory" in column_names
        finally:
            ctx.dispose()

    def test_creates_unique_index(self) -> None:
        """Unique index should have unique=True"""

        @entity
        class Product:
            id: int
            sku: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id).has_index(
                    lambda p: p.sku, unique=True
                )

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            table = getattr(Product, "__table__")
            indexes = list(table.indexes)
            assert len(indexes) == 1
            assert indexes[0].name == "uq_products_sku"
            assert indexes[0].unique
        finally:
            ctx.dispose()

    def test_creates_index_with_custom_name(self) -> None:
        """Index with custom name should use that name"""

        @entity
        class Product:
            id: int
            sku: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id).has_index(
                    lambda p: p.sku, name="idx_custom_sku"
                )

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            table = getattr(Product, "__table__")
            indexes = list(table.indexes)
            assert len(indexes) == 1
            assert indexes[0].name == "idx_custom_sku"
        finally:
            ctx.dispose()

    def test_creates_multiple_indexes(self) -> None:
        """Multiple has_index calls should create multiple indexes"""

        @entity
        class Product:
            id: int
            sku: str
            name: str
            category: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                (
                    builder.entity(Product)
                    .has_key(lambda p: p.id)
                    .has_index(lambda p: p.sku, unique=True)
                    .has_index(lambda p: p.name)
                    .has_index(lambda p: p.category)
                )

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            table = getattr(Product, "__table__")
            indexes = list(table.indexes)
            assert len(indexes) == 3
            index_names = [idx.name for idx in indexes]
            assert "uq_products_sku" in index_names
            assert "ix_products_name" in index_names
            assert "ix_products_category" in index_names
        finally:
            ctx.dispose()

    def test_index_requires_at_least_one_column(self) -> None:
        """has_index should raise error if no columns specified"""

        @entity
        class Product:
            id: int
            name: str

        class TestContext(DbContext):
            products: DbSet[Product]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(Product).has_key(lambda p: p.id).has_index()

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        with pytest.raises(ValueError, match="Must specify at least one field to index"):
            TestContext(provider)


# Define entities at module level for M:M tests to allow forward reference resolution
@entity
class MTMPost:
    id: int
    title: str
    tags: list["MTMTag"]


@entity
class MTMTag:
    id: int
    name: str


@entity
class MTMStudent:
    id: int
    name: str
    courses: list["MTMCourse"]


@entity
class MTMCourse:
    id: int
    title: str
    students: list[MTMStudent]


@entity
class MTMPost2:
    id: int
    title: str
    tags: list["MTMTag2"]


@entity
class MTMTag2:
    id: int
    name: str


class TestManyToManyRelationships:
    """Tests for many-to-many relationship functionality"""

    def test_creates_many_to_many_relationship(self) -> None:
        """Many-to-many relationship should create association table"""

        class TestContext(DbContext):
            posts: DbSet[MTMPost]
            tags: DbSet[MTMTag]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(MTMPost).has_key(lambda p: p.id).has_many(
                    lambda p: p.tags
                ).with_many()
                builder.entity(MTMTag).has_key(lambda t: t.id)

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            # verify both entity tables were created
            assert hasattr(MTMPost, "__table__")
            assert hasattr(MTMTag, "__table__")

            # verify relationship was created
            assert hasattr(MTMPost, "tags")

            # verify association table was created
            post_table = getattr(MTMPost, "__table__")
            metadata = post_table.metadata

            # association table should be named 'mtmposts_mtmtags' (alphabetical order)
            assert "mtmposts_mtmtags" in metadata.tables

            association_table = metadata.tables["mtmposts_mtmtags"]

            # verify association table has correct columns
            assert "mtmposts_id" in association_table.columns
            assert "mtmtags_id" in association_table.columns

            # verify both columns are primary keys
            assert association_table.columns["mtmposts_id"].primary_key
            assert association_table.columns["mtmtags_id"].primary_key

            # verify foreign key constraints exist
            fks = list(association_table.foreign_keys)
            assert len(fks) == 2
            fk_references = [fk.column.table.name for fk in fks]
            assert "mtmposts" in fk_references
            assert "mtmtags" in fk_references
        finally:
            ctx.dispose()

    def test_creates_bidirectional_many_to_many_relationship(self) -> None:
        """Bidirectional M:M should work with with_many"""

        class TestContext(DbContext):
            students: DbSet[MTMStudent]
            courses: DbSet[MTMCourse]

            def setup(self, builder: DbBuilder) -> None:
                # Configure both sides of the M:M relationship
                builder.entity(MTMStudent).has_key(lambda s: s.id).has_many(
                    lambda s: s.courses
                ).with_many(lambda c: c.students)
                builder.entity(MTMCourse).has_key(lambda c: c.id).has_many(
                    lambda c: c.students
                ).with_many(lambda s: s.courses)

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        ctx = TestContext(provider)
        try:
            # Verify both relationships exist
            assert hasattr(MTMStudent, "courses")
            assert hasattr(MTMCourse, "students")

            # Verify only one association table was created (shared between both sides)
            student_table = getattr(MTMStudent, "__table__")
            metadata = student_table.metadata
            assert "mtmcourses_mtmstudents" in metadata.tables
        finally:
            ctx.dispose()

    def test_many_to_many_with_foreign_key_raises_error(self) -> None:
        """M:M relationships should not allow with_foreign_key"""

        class TestContext(DbContext):
            posts: DbSet[MTMPost2]
            tags: DbSet[MTMTag2]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(MTMPost2).has_key(lambda p: p.id).has_many(
                    lambda p: p.tags
                ).with_many().with_foreign_key(
                    lambda p: p.id
                )  # Should fail
                builder.entity(MTMTag2).has_key(lambda t: t.id)

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        with pytest.raises(ValueError, match="with_foreign_key.*not applicable for many-to-many"):
            TestContext(provider)

    def test_with_many_only_on_one_to_many(self) -> None:
        """with_many() should only work on one-to-many relationships"""

        @entity
        class TestUser:
            id: int
            name: str

        @entity
        class TestPost:
            id: int
            user_id: int
            user: TestUser

        class TestContext(DbContext):
            users: DbSet[TestUser]
            posts: DbSet[TestPost]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(TestUser).has_key(lambda u: u.id)
                builder.entity(TestPost).has_key(lambda p: p.id).has_one(
                    lambda p: p.user
                ).with_many()  # should fail - has_one creates MANY_TO_ONE

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        with pytest.raises(ValueError, match="with_many.*can only be called on one-to-many"):
            TestContext(provider)


# define entities at module level for validation tests to allow forward reference resolution
@entity
class ValTestUser:
    id: int
    name: str
    posts: list["ValTestPost"]


@entity
class ValTestPost:
    id: int
    title: str
    user_id: int


@entity
class ValTest2User:
    id: int
    name: str
    posts: list["ValTest2Post"]


@entity
class ValTest2Post:
    id: int
    title: str
    # missing user_id column - intentional for FK validation test


class TestRelationshipValidation:
    """Tests for relationship validation functionality"""

    def test_validates_related_entity_is_configured(self) -> None:
        """Should raise error if related entity is not configured"""

        class TestContext(DbContext):
            users: DbSet[ValTestUser]

            def setup(self, builder: DbBuilder) -> None:
                # configure User with relationship to Post, but don't configure Post
                builder.entity(ValTestUser).has_key(lambda u: u.id).has_many(
                    lambda u: u.posts
                ).with_foreign_key(lambda p: p.user_id)

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        with pytest.raises(ValueError, match="related entity.*not configured"):
            TestContext(provider)

    def test_validates_foreign_key_column_exists(self) -> None:
        """Should raise error if foreign key column doesn't exist"""

        class TestContext(DbContext):
            users: DbSet[ValTest2User]
            posts: DbSet[ValTest2Post]

            def setup(self, builder: DbBuilder) -> None:
                builder.entity(ValTest2User).has_key(lambda u: u.id).has_many(
                    lambda u: u.posts
                ).with_foreign_key(lambda p: p.user_id)
                builder.entity(ValTest2Post).has_key(lambda p: p.id)

        provider = InMemoryProvider(InMemoryEngineOptions(use_async=False))
        with pytest.raises(AttributeError, match="Property 'user_id' does not exist"):
            TestContext(provider)
