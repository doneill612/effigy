from types import UnionType
from typing import (
    Callable,
    Type,
    TypeVar,
    Generic,
    Any,
    cast,
    get_args,
    get_type_hints,
    get_origin,
    Union,
)

from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, Float
from sqlalchemy.orm import registry

from .index import IndexConfiguration
from .property import PropertyConfiguration
from .relationship import RelationshipConfiguration, RelationshipType
from ..entity import _EntityProxy

T = TypeVar("T")

# Type mapping from Python types to SQLAlchemy types
_TYPE_MAP: dict[type, Any] = {
    int: Integer,
    str: String,
    bool: Boolean,
    float: Float,
}


class DbBuilder:
    """Fluent API for configuring effigy entities"""

    def __init__(self, metadata: MetaData):
        self._metadata = metadata
        self._entity_configs: dict[Type[Any], _EntityConfiguration[Any]] = {}

    def entity(self, entity_type: Type[T]) -> "_EntityConfiguration[T]":
        if entity_type not in self._entity_configs:
            self._entity_configs[entity_type] = _EntityConfiguration(entity_type, self)
        return self._entity_configs[entity_type]

    def _finalize(self) -> None:
        for config in self._entity_configs.values():
            config._create_table(self._metadata)
        for config in self._entity_configs.values():
            config._create_indexes()
        for config in self._entity_configs.values():
            config._create_relationships()


class _EntityConfiguration(Generic[T]):

    def __init__(self, entity_type: type[T], builder: DbBuilder):
        self._entity_type = entity_type
        self._builder = builder
        self._pks: list[str] = []
        self._properties: dict[str, PropertyConfiguration[T]] = {}
        self._relationships: list[RelationshipConfiguration[T]] = []
        self._indexes: list[IndexConfiguration] = []

    def property(self, navigation: Callable[[T], Any]) -> PropertyConfiguration[T]:
        proxy = _EntityProxy(self._entity_type)
        navattr = navigation(cast(T, proxy))
        prop_name = navattr.key

        if prop_name not in self._properties:
            self._properties[prop_name] = PropertyConfiguration(prop_name, self._entity_type, self)

        return self._properties[prop_name]

    def has_key(
        self, *navigations: Callable[[T], Any], autoincrement: bool = False
    ) -> "_EntityConfiguration[T]":
        """Marks one or more fields as a primary key.

        Args:
            navigations: lambda functions that reference the attribute(s) to make a primary key
            autoincrement: Whether or not the primary key should be treated as autoincrementing.
                This will only work for single-column primary keys, i.e. with one navigation lambda specified
        """
        if len(navigations) == 0:
            raise ValueError("Must specify at least one primary key")
        if autoincrement and len(navigations) > 1:
            raise ValueError("Autoincrement only supported on single-column primary keys")
        for navigation in navigations:
            keyname = self._get_keyname_from_navigation(navigation)
            propconfig = self._get_property_config_by_keyname(keyname)
            self._pks.append(keyname)
            if autoincrement:
                propconfig.autoincrement()
        return self

    def _get_keyname_from_navigation(self, navigation: Callable[[T], Any]) -> str:
        proxy = _EntityProxy(self._entity_type)
        keyattr = navigation(cast(T, proxy))
        return cast(str, keyattr.key)

    def _get_property_config_by_keyname(self, keyname: str) -> PropertyConfiguration[T]:
        # if keyname is in properties, return it - otherwise, create config
        if keyname in self._properties:
            return self._properties[keyname]
        return self.property(lambda e: getattr(e, keyname))

    def has_one(self, navigation: Callable[[T], Any]) -> RelationshipConfiguration[T]:
        proxy = _EntityProxy(self._entity_type)
        navattr = navigation(cast(T, proxy))
        navname = navattr.key

        rel_config = RelationshipConfiguration(
            navname, RelationshipType.MANY_TO_ONE, self._entity_type, self
        )
        self._relationships.append(rel_config)
        return rel_config

    def has_many(self, navigation: Callable[[T], Any]) -> RelationshipConfiguration[T]:
        """Configures a one-to-many relationship.

        For a many-to-many relationship, chain with .with_many():
            builder.entity(Post).has_many(lambda p: p.tags).with_many(lambda t: t.posts)

        Args:
            navigation: lambda function that references the collection attribute

        Returns:
            RelationshipConfiguration for further configuration (with_many, with_foreign_key,
            backpopulates, cascade, etc.)
        """
        proxy = _EntityProxy(self._entity_type)
        navattr = navigation(cast(T, proxy))
        navname = navattr.key

        rel_config = RelationshipConfiguration(
            navname, RelationshipType.ONE_TO_MANY, self._entity_type, self
        )
        self._relationships.append(rel_config)
        return rel_config

    def has_index(
        self, *navigations: Callable[[T], Any], unique: bool = False, name: str | None = None
    ) -> "_EntityConfiguration[T]":
        """Creates an index on one or more fields.

        Args:
            navigations: lambda functions that reference the attribute(s) to index
            unique: Whether this should be a unique index
            name: Optional custom name for the index. If not provided, a name will be auto-generated

        Returns:
            Self for method chaining
        """
        if len(navigations) == 0:
            raise ValueError("Must specify at least one field to index")

        field_names = [self._get_keyname_from_navigation(nav) for nav in navigations]
        index_config = IndexConfiguration(field_names, unique=unique, name=name)
        self._indexes.append(index_config)
        return self

    def _validate_autoincrement(
        self, field_name: str, field_type: Type[Any], *, autoincrement: bool
    ) -> None:
        if not autoincrement:
            return

        origin = get_origin(field_type)
        if origin is Union or origin is UnionType:
            args = get_args(field_type)
            # need to type autoincrementing primary keys as int | None
            if type(None) in args and int in args:
                return

        raise TypeError(
            f"Database-generated values require an optional type. "
            f"Autoincrementing field {field_name} must be defined as int | None"
        )

    def _create_table(self, metadata: MetaData) -> None:
        """Creates a SQLAlchemy Table and attaches it to the entity class.

        This method:
        1. Introspects the entity class annotations to discover fields
        2. Creates SQLAlchemy Column objects for each field
        3. Builds a Table object with the columns
        4. Attaches the table to the entity class as __table__
        5. Uses imperative mapping to register the entity with SQLAlchemy
        """

        # this should definitely be set already...
        table_name = getattr(self._entity_type, "__tablename__")
        if not table_name:
            raise ValueError(f"Entity {self._entity_type.__name__} has no table name configured")

        try:
            type_hints = get_type_hints(self._entity_type)
        except Exception:
            # fallback to __annotations__ if get_type_hints fails
            type_hints = getattr(self._entity_type, "__annotations__", {})

        if not self._pks:
            raise ValueError(f"Table {table_name} must have at least one primary key")

        columns = []
        for field_name, field_type in type_hints.items():
            # skip private attributes and special attributes
            if field_name.startswith("_"):
                continue

            origin = get_origin(field_type)
            # these are likely relationships
            # TODO: maybe add support for dict as JSONB/JSON col types later
            if origin in (list, dict, set):
                continue

            # Check for optionality and validate union types
            is_nullable = False
            nonnull_type = field_type

            origin = get_origin(field_type)
            if origin is type(None):  # Direct None type annotation
                continue
            elif origin is Union or (
                hasattr(field_type, "__args__") and len(getattr(field_type, "__args__", ())) > 1
            ):
                # This is a Union type (including Optional which is Union[T, None])
                args = getattr(field_type, "__args__", ())

                # Check if None is in the union (making it Optional)
                if type(None) in args:
                    # Remove None to get the actual type(s)
                    non_none_args = [arg for arg in args if arg is not type(None)]

                    # If there's more than one non-None type, it's still an invalid union
                    if len(non_none_args) > 1:
                        raise TypeError(
                            f"Entity {self._entity_type.__name__} field '{field_name}' has unsupported union type: {field_type}. "
                            f"Union types (other than Optional[T]) are not supported in ORMs. "
                            f"Database columns must have a single type. "
                            f"Use Optional[T] for nullable columns, not Union[T, U, ...]."
                        )

                    # Valid Optional type
                    is_nullable = True
                    nonnull_type = non_none_args[0] if non_none_args else field_type
                else:
                    # Union without None - completely unsupported
                    raise TypeError(
                        f"Entity {self._entity_type.__name__} field '{field_name}' has unsupported union type: {field_type}. "
                        f"Union types are not supported in ORMs. "
                        f"Database columns must have a single type. "
                        f"If the field is optional, use Optional[T] or T | None."
                    )

            # default to string if we can't deduce what the type is
            sa_type = _TYPE_MAP.get(nonnull_type, String)

            is_primary = field_name in self._pks

            # additional configuration from property config (if present)
            prop_config = self._properties.get(field_name)
            if prop_config:
                nullable = is_nullable and not prop_config._required
                unique = prop_config.is_unique
                default = prop_config.default
                autoincrement = prop_config.is_autoincrement
                server_default = prop_config.server_default

                # apply max_length to string-type cols
                if prop_config._max_length is not None:
                    # max_length should only be used on string-type cols
                    if nonnull_type != str:
                        raise ValueError(
                            f"Entity {self._entity_type.__name__} field '{field_name}': "
                            f"max_len() can only be used on string (str) fields, not {nonnull_type.__name__}"
                        )
                    sa_type = String(prop_config._max_length)
            else:
                # default configuration values (assumed)
                nullable = is_nullable and not is_primary
                unique = False
                default = None
                autoincrement = False
                server_default = None

            self._validate_autoincrement(field_name, field_type, autoincrement=autoincrement)
            col = Column(
                field_name,
                sa_type,
                primary_key=is_primary,
                nullable=nullable,
                unique=unique,
                default=default,
                server_default=server_default,
                autoincrement=autoincrement if autoincrement else "auto",
            )
            columns.append(col)

        table = Table(table_name, metadata, *columns)

        setattr(self._entity_type, "__table__", table)

        # Use imperative mapping to register the entity with SQLAlchemy's ORM
        # This enables queries, change tracking, relationships, and all ORM features
        # Only map if not already mapped (to support multiple context instances)
        if not hasattr(self._entity_type, "__mapper__"):
            mapper_reg = registry()
            mapper_reg.map_imperatively(self._entity_type, table)

    def _create_indexes(self) -> None:
        """Creates SQLAlchemy Index objects for all configured indexes.

        This method applies all configured indexes by calling create_index() on each
        IndexConfiguration. The indexes are created on the table that was previously
        created by _create_table().
        """
        table = getattr(self._entity_type, "__table__", None)
        if table is None:
            raise ValueError(f"Entity {self._entity_type.__name__} has no table configured")

        table_name = getattr(self._entity_type, "__tablename__")
        for index_config in self._indexes:
            index_config.create_index(table, table_name)

    def _create_relationships(self) -> None:
        """Creates SQLAlchemy relationship() objects and attaches them to entity classes.

        This method applies all configured relationships by calling _apply() on each
        RelationshipConfiguration. The _apply() method:
        1. Creates a SQLAlchemy relationship() with configured options (cascade, lazy, etc.)
        2. Attaches the relationship to the entity class as a dynamic attribute
        3. Handles foreign key references if specified
        4. Sets up bidirectional relationships via back_populates if configured
        """
        for rel_config in self._relationships:
            self._validate_relationship(rel_config)
            rel_config._apply()

    def _validate_relationship(self, rel_config: RelationshipConfiguration[T]) -> None:
        """Validates a relationship configuration before applying it.

        This checks:
        1. That the related entity is also configured in the builder
        2. For non-M:M relationships, that the foreign key column exists

        Args:
            rel_config: The relationship configuration to validate

        Raises:
            ValueError: If validation fails
        """
        # determine the related entity if not already set
        if not rel_config._related_entity:
            rel_config._related_entity = rel_config._determine_related_entity()

        related_entity = rel_config._related_entity

        # validate that the related entity is configured in the builder
        if related_entity not in self._builder._entity_configs:
            raise ValueError(
                f"Cannot create relationship '{rel_config._navigation_name}' on "
                f"{self._entity_type.__name__}. The related entity "
                f"{related_entity.__name__} is not configured in the builder. "
                f"Please configure it using builder.entity({related_entity.__name__})."
            )

        if rel_config._relationship_type != RelationshipType.MANY_TO_MANY:
            if rel_config._fk_prop:
                # determine which entity owns the FK
                if rel_config._relationship_type == RelationshipType.ONE_TO_MANY:
                    fk_owner = related_entity
                else:
                    fk_owner = self._entity_type

                fk_table = getattr(fk_owner, "__table__", None)
                if fk_table is None:
                    raise ValueError(
                        f"Cannot validate foreign key for relationship "
                        f"'{rel_config._navigation_name}'. The entity "
                        f"{fk_owner.__name__} has no table configured."
                    )

                if rel_config._fk_prop not in fk_table.columns:
                    raise ValueError(
                        f"Cannot create relationship '{rel_config._navigation_name}' on "
                        f"{self._entity_type.__name__}. The foreign key column "
                        f"'{rel_config._fk_prop}' does not exist on {fk_owner.__name__}. "
                        f"Available columns: {', '.join(fk_table.columns.keys())}"
                    )
