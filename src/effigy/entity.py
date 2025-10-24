from typing import Type, TypeVar, Protocol, Any, get_origin, cast
from typing_extensions import dataclass_transform
from attrs import define, field

T = TypeVar("T")
TEntity = TypeVar("TEntity", bound="Entity[Any]")


class Entity(Protocol[T]):
    """Protocol for effigy entity classes.

    Entities are classes decorated with @entity that can be managed by a DbContext.
    The __tablename__ attribute is set by the decorator.
    The __table__ attribute is set during DbContext initialization when the builder runs.
    """

    # set by the @entity decorator
    __tablename__: str
    # set by the DbBuilder during DbContext initialization
    # optional because it's not present until builder.finalize() is called
    __table__: Any | None

    # preserve reference to the original type
    __effigy_entity_type__: Type[T]


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


def _is_attrs_entity(c: Type[Any]) -> bool:
    return hasattr(c, "__effigy_entity__") or hasattr(c, "__attrs_attrs__")


def _is_pydantic_entity(c: Type[Any]) -> bool:
    try:
        from pydantic import BaseModel

        return issubclass(c, BaseModel)
    except ImportError:
        return False


def validate_entity(entity: Any, entity_type: Type[Any]) -> None:
    if _is_attrs_entity(entity_type):
        from attrs import validate

        validate(entity)
    elif _is_pydantic_entity(entity_type):
        # forced re-validation... pydantic validates on instantiation
        entity.model_validate(entity)
    else:
        raise TypeError(
            f"{entity_type.__name__} is not a valid effigy entity. "
            f"Use the @entity decorator or inherit from a Pydantic BaseModel."
        )


def _pluralize(s: str) -> str:
    if s.endswith("y") and len(s) > 1 and s[-2] not in "aeiou":
        return s[:-1] + "ies"
    elif s.endswith("z") and len(s) > 1 and s[-2] not in "aeiou":
        return s + "zes"
    elif s.endswith(("s", "x", "z", "ch", "sh")):
        return s + "es"
    return s + "s"
