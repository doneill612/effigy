from typing import Callable, Type, TypeVar, Generic, Any, get_type_hints, get_origin, Union

from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, Float
from sqlalchemy.orm import InstrumentedAttribute, registry

from .index import IndexConfiguration
from .property import PropertyConfiguration
from .relationship import RelationshipConfiguration

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
        self._entity_configs: dict[Type, EntityConfiguration[Any]] = {}

    def entity(self, entity_type: Type[T]) -> "EntityConfiguration[T]":
        if entity_type not in self._entity_configs:
            self._entity_configs[entity_type] = EntityConfiguration(entity_type, self)
        return self._entity_configs[entity_type]

    def finalize(self) -> None:
        for config in self._entity_configs.values():
            config._create_table(self._metadata)
        for config in self._entity_configs.values():
            config._create_relationships()


class EntityConfiguration(Generic[T]):

    def __init__(self, entity_type: type[T], builder: DbBuilder):
        self._entity_type = entity_type
        self._builder = builder
        self._pks: list[str] = []
        self._properties: dict[str, PropertyConfiguration] = {}
        self._relationships: list[RelationshipConfiguration] = []
        self._indexes: list[IndexConfiguration] = []

    def property(
        self, navigation: Callable[[Type[T]], InstrumentedAttribute[object]]
    ) -> PropertyConfiguration[T]:
        prop_attr = navigation(self._entity_type)
        prop_name = prop_attr.key

        if prop_name not in self._properties:
            self._properties[prop_name] = PropertyConfiguration(prop_name, self._entity_type, self)

        return self._properties[prop_name]

    def has_key(
        self, navigation: Callable[[Type[T]], InstrumentedAttribute[object]] | str
    ) -> "EntityConfiguration[T]":
        """Marks a field as a primary key.

        Args:
            navigation: Either a lambda function (lambda u: u.id) or a string field name ("id")
        """
        if isinstance(navigation, str):
            key_name = navigation
        else:
            key_attr = navigation(self._entity_type)
            key_name = key_attr.key

        self._pks.append(key_name)
        return self

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
                # TODO: expose readonly props here instead of going right at the private field
                unique = prop_config._unique
                default = prop_config._default
            else:
                # default configuration values (assumed)
                nullable = is_nullable and not is_primary
                unique = False
                default = None

            # TODO: autoincrement support
            col = Column(
                field_name,
                sa_type,
                primary_key=is_primary,
                nullable=nullable,
                unique=unique,
                default=default,
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
