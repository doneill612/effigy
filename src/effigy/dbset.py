from typing import (
    AsyncIterator,
    Iterable,
    Iterator,
    Type,
    TypeVar,
    Generic,
    Callable,
    Any,
    TYPE_CHECKING,
    cast,
)
from sqlalchemy import ColumnElement, ResultProxy, select, update


from .entity import _EntityProxy


if TYPE_CHECKING:
    from .context import DbContext, AsyncDbContext

from .qb import QueryBuilder, AsyncQueryBuilder

T = TypeVar("T")


class DbSet(Generic[T]):
    """Database set for querying and managing entities.

    The entity_type must conform to the Entity protocol (have __tablename__ and __table__).
    This is validated at runtime during initialization.
    """

    def __init__(self, entity_type: Type[T], context: "DbContext"):
        # Runtime validation that the type conforms to Entity protocol
        if not hasattr(entity_type, "__tablename__"):
            raise TypeError(
                f"{entity_type.__name__} does not conform to Entity protocol: "
                f"missing __tablename__ attribute. Did you forget the @entity decorator?"
            )

        # Note: __table__ might be None initially, will be set by builder
        if not hasattr(entity_type, "__table__"):
            raise TypeError(
                f"{entity_type.__name__} does not conform to Entity protocol: "
                f"missing __table__ attribute. Did you forget the @entity decorator?"
            )

        self._entity_type = entity_type
        self._context = context

    def add(self, entity: T) -> T:
        self._context._get_session().add(entity)
        return entity

    def add_range(self, entities: Iterable[T]) -> None:
        elist = list(entities)
        self._context._get_session().bulk_save_objects(elist)

    def update_where(self, predicate: Callable[[T], bool], **updates: Any) -> None:
        proxy = _EntityProxy(self._entity_type)
        filterexpr = predicate(cast(T, proxy))
        statement = (
            # we know that __table__ will exist by now
            update(getattr(self._entity_type, "__table__"))
            .where(cast(ColumnElement[bool], filterexpr))
            .values(**updates)
        )
        self._context._get_session().execute(statement)

    def remove(self, entity: T) -> None:
        self._context._get_session().delete(entity)

    def where(self, predicate: Callable[[T], bool]) -> QueryBuilder[T]:
        qb = QueryBuilder(self._entity_type, self._context._get_session())
        return qb.where(predicate)

    def include(self, navigation: Callable[[T], Any]) -> QueryBuilder[T]:
        qb = QueryBuilder(self._entity_type, self._context._get_session())
        return qb.include(navigation)

    def to_list(self) -> list[T]:
        return self._context._get_session().query(self._entity_type).all()

    def __iter__(self) -> Iterator[T]:
        return iter(self.to_list())


class AsyncDbSet(Generic[T]):
    """Async database set for querying and managing entities.

    The entity_type must conform to the Entity protocol (have __tablename__ and __table__).
    This is validated at runtime during initialization.
    """

    def __init__(self, entity_type: Type[T], context: "AsyncDbContext"):
        # Runtime validation that the type conforms to Entity protocol
        if not hasattr(entity_type, "__tablename__"):
            raise TypeError(
                f"{entity_type.__name__} does not conform to Entity protocol: "
                f"missing __tablename__ attribute. Did you forget the @entity decorator?"
            )

        if not hasattr(entity_type, "__table__"):
            raise TypeError(
                f"{entity_type.__name__} does not conform to Entity protocol: "
                f"missing __table__ attribute. Did you forget the @entity decorator?"
            )

        self._entity_type = entity_type
        self._context = context

    def add(self, entity: T) -> T:
        self._context._get_session().add(entity)
        return entity

    async def remove(self, entity: T) -> None:
        await self._context._get_session().delete(entity)

    def where(self, predicate: Callable[[T], bool]) -> AsyncQueryBuilder[T]:
        qb = AsyncQueryBuilder(self._entity_type, self._context._get_session())
        return qb.where(predicate)

    def include(self, navigation: Callable[[T], Any]) -> AsyncQueryBuilder[T]:
        qb = AsyncQueryBuilder(self._entity_type, self._context._get_session())
        return qb.include(navigation)

    async def to_list(self) -> list[T]:
        exc = await self._context._get_session().execute(select(self._entity_type))
        return list(exc.scalars())

    def __aiter__(self) -> AsyncIterator[T]:
        return self._async_iterator()

    async def _async_iterator(self) -> AsyncIterator[T]:
        entities = await self.to_list()
        for entity in entities:
            yield entity
