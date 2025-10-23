from typing import Type, TypeVar, Generic, Callable, Any, TYPE_CHECKING
from sqlalchemy import ColumnElement, select


if TYPE_CHECKING:
    from .context import DbContext, AsyncDbContext

from .qb import QueryBuilder, AsyncQueryBuilder
from .entity import Entity

T = TypeVar("T", bound=Entity)


class DbSet(Generic[T]):

    def __init__(self, entity_type: Type[T], context: "DbContext"):
        self._entity_type = entity_type
        self._context = context

    def add(self, entity: T) -> T:
        self._context.session.add(entity)
        return entity

    def remove(self, entity: T) -> None:
        self._context.session.delete(entity)

    def where(self, predicate: Callable[[Type[T]], ColumnElement[bool]]) -> QueryBuilder[T]:
        qb = QueryBuilder(self._entity_type, self._context.session)
        return qb.where(predicate)

    def include(self, navigation: Callable[[Type[T]], Any]) -> QueryBuilder[T]:
        qb = QueryBuilder(self._entity_type, self._context.session)
        return qb.include(navigation)

    def to_list(self) -> list[T]:
        return self._context.session.query(self._entity_type).all()

    def __iter__(self):
        return iter(self.to_list())


class AsyncDbSet(Generic[T]):

    def __init__(self, entity_type: Type[T], context: "AsyncDbContext"):
        self._entity_type = entity_type
        self._context = context

    def add(self, entity: T) -> T:
        self._context.session.add(entity)
        return entity

    async def remove(self, entity: T) -> None:
        await self._context.session.delete(entity)

    def where(self, predicate: Callable[[Type[T]], ColumnElement[bool]]) -> AsyncQueryBuilder[T]:
        qb = AsyncQueryBuilder(self._entity_type, self._context.session)
        return qb.where(predicate)

    def include(self, navigation: Callable[[Type[T]], Any]) -> AsyncQueryBuilder[T]:
        qb = AsyncQueryBuilder(self._entity_type, self._context.session)
        return qb.include(navigation)

    async def to_list(self) -> list[T]:
        exc = await self._context.session.execute(select(self._entity_type))
        return list(exc.scalars())

    async def __aiter__(self):
        return self._async_iterator()

    async def _async_iterator(self):
        entities = await self.to_list()
        for entity in entities:
            yield entity
