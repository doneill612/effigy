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
        return keyattr.key

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
        proxy = _EntityProxy(self._entity_type)
        navattr = navigation(cast(T, proxy))
        navname = navattr.key

        rel_config = RelationshipConfiguration(
            navname, RelationshipType.ONE_TO_MANY, self._entity_type, self
        )
        self._relationships.append(rel_config)
        return rel_config

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
            else:
                # default configuration values (assumed)
                nullable = is_nullable and not is_primary
                unique = False
                default = None
                autoincrement = False

            self._validate_autoincrement(field_name, field_type, autoincrement=autoincrement)
            col = Column(
                field_name,
                sa_type,
                primary_key=is_primary,
                nullable=nullable,
                unique=unique,
                default=default,
                autoincrement=autoincrement if autoincrement else "auto",
            )
            columns.append(col)

        if not any(col.primary_key for col in columns):
            col_info = [(c.name, c.primary_key) for c in columns]
            raise ValueError(
                f"Entity {self._entity_type.__name__} must have at least one primary key. "
                f"Use .has_key() in the builder configuration. "
                f"PKs configured: {self._pks}, Columns: {col_info}"
            )

        table = Table(table_name, metadata, *columns)

        setattr(self._entity_type, "__table__", table)

        # Use imperative mapping to register the entity with SQLAlchemy's ORM
        # This enables queries, change tracking, relationships, and all ORM features
        # Only map if not already mapped (to support multiple context instances)
        if not hasattr(self._entity_type, "__mapper__"):
            mapper_reg = registry()
            mapper_reg.map_imperatively(self._entity_type, table)

    def _create_relationships(self) -> None:
        """Creates SQLAlchemy relationship() objects and attaches them to entity classes.

        This method applies all configured relationships by calling _apply() on each
        RelationshipConfiguration. The _apply() method:
        1. Creates a SQLAlchemy relationship() with configured options (cascade, lazy, etc.)
        2. Attaches the relationship to the entity class as a dynamic attribute
        3. Handles foreign key references if specified
        4. Sets up bidirectional relationships via back_populates if configured

        Note: This must be called after _create_table() because relationships may
        reference foreign key columns that need to exist first.
        """
        for rel_config in self._relationships:
            # TODO: add validation to ensure related entity is also configured
            # TODO: add validation for foreign key column existence
            rel_config._apply()
