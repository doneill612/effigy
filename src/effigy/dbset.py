from typing import Type, TypeVar, Generic, Callable
from typing_extensions import Self


from .context import DbContext, AsyncDbContext
from .qb import QueryBuilder

T = TypeVar("T")
TEntity = TypeVar("TEntity", bound="DbSet")


# NOTE: These dbsets should conform to the Queryable protocol


class DbSet(Generic[T]):

    def __init__(self, entity_type: Type[T], context: DbContext):
        self._entity_type = entity_type
        self._context = context
        self._query_builder: QueryBuilder[T] | None = None

    def add(self, entity: T) -> T:
        self._context.session.add(entity)
        return entity

    def remove(self, entity: T) -> None:
        return self._context.session.delete(entity)

    def where(self, predicate: Callable[[T], bool]) -> QueryBuilder[T]:
        qb = QueryBuilder(self._entity_type, self._context.session)
        return qb.where(predicate)


class AsyncDbSet(Generic[T]):

    def __init__(self, entity_type: Type[T], context: AsyncDbContext):
        self._entity_type = entity_type
        self._context = context
        self._query_builder: QueryBuilder[T] | None = None

    def add(self, entity: T) -> T:
        self._context.session.add(entity)
        return entity

    async def remove(self, entity: T) -> None:
        return await self._context.session.delete(entity)
