from typing import Generic, Type, TypeVar, Protocol, Any, get_origin, cast
from typing_extensions import dataclass_transform
from attrs import define, field

T = TypeVar("T")
TEntity = TypeVar("TEntity", bound="Entity[Any]")


class Entity(Protocol[T]):
    """Protocol for effigy entity classes.

    Entities are classes decorated with `@entity` that can be managed by a DbContext.
    The `__tablename__` attribute is set by the decorator.
    The `__effigy_entity_type__` is set by the decorator.
    The `__table__` attribute is set during DbContext initialization when the builder runs.
    """

    # set by the @entity decorator
    __tablename__: str
    # set by the DbBuilder during DbContext initialization
    # optional because it's not present until builder.finalize() is called
    __table__: Any | None

    # preserve reference to the original type
    __effigy_entity_type__: Type[T]


class _MockAttribute:
    """Mock that mimics SQLAlchemy's InstrumentedAttribute interface."""

    def __init__(self, key: str):
        self.key = key


class _EntityProxy(Generic[T]):
    """Proxy object that captures attribute access for type-safe navigation lambdas.

    This proxy is used during entity configuration (before SQLAlchemy mapping) to
    enable lambda-based navigation like `lambda p: p.email` instead of string-based
    navigation like `"email"`.

    The proxy validates that accessed attributes exist as declared properties on the
    entity type and prevents attribute mutations during navigation.
    """

    def __init__(self, entity_type: Type[T]):
        object.__setattr__(self, "_entity_type", entity_type)
        # forward references in annotations are fine since we only need field names for validation
        type_hints = getattr(entity_type, "__annotations__", {})
        object.__setattr__(self, "_type_hints", type_hints)

    def __getattribute__(self, name: str) -> Any:
        # we want to allow access to internal attributes
        if name.startswith("_"):
            return object.__getattribute__(self, name)

        entity_type: Type[T] = object.__getattribute__(self, "_entity_type")
        type_hints: dict[str, Any] = object.__getattribute__(self, "_type_hints")

        if name not in type_hints:
            available = ", ".join(type_hints.keys())
            raise AttributeError(
                f"Property '{name}' does not exist on entity {entity_type.__name__}. "
                f"Available properties: {available or '(none)'}"
            )

        return _MockAttribute(name)

    def __setattr__(self, name: str, value: Any) -> None:
        entity_type: Type[T] = object.__getattribute__(self, "_entity_type")
        raise AttributeError(
            f"Cannot set attribute '{name}={value}' during navigation on {entity_type.__name__}. "
            f"Navigation lambdas should only read attributes, not modify them."
        )


@dataclass_transform(kw_only_default=False, field_specifiers=(field,))
def entity(c: Type[T]) -> Type[Entity[T]]:
    """A decorator that can be used to define an effigy class.

    Internally provides:
        - automatic constructor generation
        - field validation
        - immutability options

    This decorator automatically:
        - sets up field factories for collection types
        - applies table name conventions if a __tablename__ is not explicitly provided
        - ensures the decorated class satisfies the entity protocol

    Example:
        >>> @entity
        ... class User:
        ...     id: int | None
        ...     name: str
        ...     email: str
        ...     posts: list["Post"]

    Args:
        c: The class to convert into an entity
    Returns
        A decorated class that conforms to the effigy entity protocol
    """

    annotations = c.__annotations__ if hasattr(c, "__annotations__") else {}

    for attr_name, attr_type in annotations.items():
        if not hasattr(c, attr_name):
            origin = get_origin(attr_type)
            # get_origin returns the unparameterized version of generic types
            # e.g., list for list[int], dict for dict[str, int], etc.
            if origin is list:  # type: ignore[comparison-overlap]
                setattr(c, attr_name, field(factory=list))
            elif origin is dict:  # type: ignore[comparison-overlap]
                setattr(c, attr_name, field(factory=dict))
            elif origin is set:  # type: ignore[comparison-overlap]
                setattr(c, attr_name, field(factory=set))

    effigy_cls = define(c, kw_only=False, slots=False)
    if not hasattr(effigy_cls, "__tablename__"):
        setattr(effigy_cls, "__tablename__", _pluralize(c.__name__.lower()))

    # Initialize __table__ to None - will be set by DbBuilder during context init
    if not hasattr(effigy_cls, "__table__"):
        setattr(effigy_cls, "__table__", None)

    setattr(effigy_cls, "__effigy_entity__", True)
    setattr(effigy_cls, "__effigy_entity_type__", c)

    # attrs.define generates __init__ from annotations
    # dataclass_transform decorator tells mypy how to handle this
    return cast(type[Entity[T]], effigy_cls)


def _pluralize(s: str) -> str:
    if s.endswith("y") and len(s) > 1 and s[-2] not in "aeiou":
        return s[:-1] + "ies"
    elif s.endswith("z") and len(s) > 1 and s[-2] not in "aeiou":
        return s + "zes"
    elif s.endswith(("s", "x", "z", "ch", "sh")):
        return s + "es"
    return s + "s"
