from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar, Any, cast, Type

from sqlalchemy import Select, select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.orm.base import InspectionAttr
from sqlalchemy.orm.strategy_options import Load
from sqlalchemy.sql import ColumnElement
from typing_extensions import Self


T = TypeVar("T")


@dataclass
class _IncludeChain:
    root: Any
    thens: list[Callable[[Type[Any]], Any]]

    def to_load_opts(self) -> Load:
        load = cast(Load, joinedload(self.root))
        final = load
        for then in self.thens:
            tmapper = final.path[-1]
            # tmapper.entity returns the mapped class for relationships
            # we know this will have .entity because it's a relationship path
            if hasattr(tmapper, "entity"):
                nested = then(tmapper.entity)  # type: ignore[arg-type]
                final = final.joinedload(nested)
        return final


class _QueryBuilderBase(Generic[T]):
    """Base class for all query builders containing shared query building logic."""

    def __init__(self, entity_type: type[T], session: Session | AsyncSession):
        self._entity_type = entity_type
        self._session = session
        self._statement: Select[tuple[T]] = select(entity_type)
        self._includes: list[_IncludeChain] = []
        self._chain: _IncludeChain | None = None

    def where(self, predicate: Callable[[Type[T]], ColumnElement[bool]]) -> Self:
        filter_expr = predicate(self._entity_type)

        self._statement = self._statement.where(filter_expr)
        return self

    def include(self, navigation: Callable[[Type[T]], Any]) -> Self:

        relationship = navigation(self._entity_type)

        self._chain = _IncludeChain(root=relationship, thens=[])
        self._includes.append(self._chain)

        return self

    def then_include(self, navigation: Callable[[Type[Any]], Any]) -> Self:
        if not self._chain:
            raise RuntimeError("then_include(...) must be called after include(...)")
        self._chain.thens.append(navigation)
        return self

    def order_by(self, key: Callable[[Type[T]], Any], *, desc: bool = False) -> Self:
        column = key(self._entity_type)
        self._statement = self._statement.order_by(column if not desc else column.desc())
        return self

    def _compile(self) -> Select[tuple[T]]:
        statement = self._statement
        for include in self._includes:
            load = include.to_load_opts()
            statement = statement.options(load)
        return statement

    def skip(self, count: int) -> Self:
        self._statement = self._statement.offset(count)
        return self

    def take(self, count: int) -> Self:
        self._statement = self._statement.limit(count)
        return self

    def distinct(self) -> Self:
        self._statement = self._statement.distinct()
        return self


class QueryBuilder(_QueryBuilderBase[T]):
    """Synchronous query builder utility"""

    def __init__(self, entity_type: Type[T], session: Session):
        super().__init__(entity_type, session)
        # for typehinting purposes, reassign to narrow type
        self._session: Session = session

    def to_list(self) -> list[T]:
        statement = self._compile()
        return list(self._session.execute(statement).scalars())

    def first(self, *, default: T | None = None) -> T:
        statement = self._compile()
        result = self._session.execute(statement).scalars().first()
        if result is None:
            if default:
                return default
            raise ValueError("Statement didn't return any results, and no default was supplied")
        return result

    def single(self) -> T:
        results = self.to_list()
        if len(results) == 0:
            raise ValueError("Statement didn't return any results")
        if len(results) > 0:
            raise ValueError(f"Expected one result, found {len(results)}")
        return results[0]

    def count(self) -> int:
        statement = self._compile()
        count_statement = select(func.count()).select_from(statement.subquery())
        return self._session.execute(count_statement).scalar() or 0

    def any(self) -> bool:
        return self.count() > 0


class AsyncQueryBuilder(_QueryBuilderBase[T]):
    """Asynchronous query builder utility"""

    def __init__(self, entity_type: Type[T], session: AsyncSession):
        super().__init__(entity_type, session)
        # for typehinting purposes, reassign to narrow type
        self._session: AsyncSession = session

    async def to_list(self) -> list[T]:
        statement = self._compile()
        exc = await self._session.execute(statement)
        result = exc.scalars()
        return list(result)

    async def first(self, *, default: T | None = None) -> T:
        statement = self._compile()
        exc = await self._session.execute(statement)
        result = exc.scalars().first()
        if result is None:
            if default:
                return default
            raise ValueError("Statement didn't return any results, and no default was supplied")
        return result

    async def single(self) -> T:
        results = await self.to_list()
        if len(results) == 0:
            raise ValueError("Statement didn't return any results")
        if len(results) > 0:
            raise ValueError(f"Expected one result, found {len(results)}")
        return results[0]

    async def count(self) -> int:
        statement = self._compile()
        count_statement = select(func.count()).select_from(statement.subquery())
        exc = await self._session.execute(count_statement)
        return exc.scalar() or 0

    async def any(self) -> bool:
        return await self.count() > 0
