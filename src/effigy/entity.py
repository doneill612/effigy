from typing import Generic, Type, TypeVar, Protocol, Callable, Any, get_origin
from typing_extensions import Self
from attrs import define, field, validators

T = TypeVar("T")
TEntity = TypeVar("TEntity", bound="Entity")


class Entity(Protocol):
    """An entity is a tracked and managed database record."""

    __tablename__: str
    # SQLAlchemy table will be returned by the model builder...
    __table__: Any


class Queryable(Protocol[T]):
    """Protocol for queryable collections"""

    def where(self, predicate: Callable[[T], bool]) -> Self: ...
    def order_by(self, predicate: Callable[[T], bool]) -> Self: ...
    def first(self, predicate: Callable[[T], bool]) -> T: ...
    def to_list(self, predicate: Callable[[T], bool]) -> list[T]: ...


def entity(c: Type[T]) -> Type[T]:
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
            if isinstance(origin, list):
                setattr(c, attr_name, field(factory=list))
            elif isinstance(origin, dict):
                setattr(c, attr_name, field(factory=dict))
            elif isinstance(origin, set):
                setattr(c, attr_name, field(factory=set))

    effigy_cls = define(c, kw_only=False, slots=True)
    if not hasattr(c, "__tablename__"):
        setattr(c, "__tablename__", _pluralize(c.__name__.lower()))

    setattr(c, "__effigy_entity__", True)

    return effigy_cls


def _pluralize(s: str) -> str:
    if s.endswith("y") and len(s) > 1 and s[-2] not in "aeiou":
        return s[:-1] + "ies"
    elif s.endswith(("s", "x", "z", "ch", "sh")):
        return s + "es"
    return s + "s"
