from typing import Generic, TypeVar, Protocol, Callable, Any


T = TypeVar("T")
TEntity = TypeVar("TEntity", bound="Entity")


class Entity(Protocol):
    """An entity is a tracked and managed database record."""

    __tablename__: str
    __table__: Any


class Queryable(Protocol[T]): ...
