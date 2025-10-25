from abc import ABC, abstractmethod
from typing import get_origin, get_args, Any
from typing_extensions import Self

from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session

from .builder.core import DbBuilder
from .dbset import AsyncDbSet, DbSet
from .provider.base import DatabaseProvider


class DbContext(ABC):
    """Synchronous database context"""

    def __init__(self, provider: DatabaseProvider[Any]):
        connection_string = provider.get_connection_string()
        opts = provider.get_engine_options()

        self._engine = create_engine(connection_string, **opts)
        self._session_factory = sessionmaker(bind=self._engine)
        self._session: Session | None = None
        self._init_dbsets()

    @abstractmethod
    def setup(self, builder: DbBuilder) -> None: ...

    def _get_session(self) -> Session:
        """Internal method to get or create the session. Not part of public API."""
        if not self._session:
            self._session = self._session_factory()
        return self._session

    def _init_dbsets(self) -> None:
        metadata = MetaData()
        builder = DbBuilder(metadata)

        self.setup(builder)

        builder._finalize()

        metadata.create_all(self._engine, checkfirst=True)

        for name, annotation in self.__annotations__.items():
            if get_origin(annotation) is DbSet:
                entity_type = get_args(annotation)[0]
                dbset = DbSet(entity_type, self)
                setattr(self, name, dbset)

    def save_changes(self) -> int:
        """Attempts to persist changes to the database.

        Returns:
            The total number of tracked changes in this operation
        """
        try:
            session = self._get_session()
            session.flush()
            # capture counts before commit clears them
            change_count = len(session.dirty) + len(session.new) + len(session.deleted)
            session.commit()
            return change_count
        except Exception as e:
            self._get_session().rollback()
            raise Exception("Something went wrong when saving changes to the database") from e

    def dispose(self) -> None:
        if self._session:
            self._session.close()
        self._engine.dispose()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        if exc_type is None:
            self.save_changes()
        else:
            self._get_session().rollback()
        self._get_session().close()


class AsyncDbContext(ABC):
    """Asynchronous database context"""

    def __init__(self, provider: DatabaseProvider[Any]):
        connection_string = provider.get_connection_string()
        opts = provider.get_engine_options()

        self._engine = create_async_engine(connection_string, **opts)
        self._session_factory = async_sessionmaker(bind=self._engine, class_=AsyncSession)
        self._session: AsyncSession | None = None
        self._metadata = self._init_dbsets()

    def _get_session(self) -> AsyncSession:
        """Internal method to get the session. Not part of public API."""
        if not self._session:
            raise RuntimeError("Session not initialized. Use `async with`.")
        return self._session

    @abstractmethod
    def setup(self, builder: DbBuilder) -> None: ...

    def _init_dbsets(self) -> MetaData:
        metadata = MetaData()
        builder = DbBuilder(metadata)

        self.setup(builder)

        builder._finalize()

        for name, annotation in self.__annotations__.items():
            if get_origin(annotation) is AsyncDbSet:
                entity_type = get_args(annotation)[0]
                dbset = AsyncDbSet(entity_type, self)
                setattr(self, name, dbset)

        return metadata

    async def save_changes(self) -> int:
        """Attempts to persist changes to the database.

        Returns:
            The total number of tracked changes in this operation
        """
        try:
            session = self._get_session()
            await session.flush()
            # capture counts before commit clears them
            change_count = len(session.dirty) + len(session.new) + len(session.deleted)
            await session.commit()
            return change_count
        except Exception as e:
            await self._get_session().rollback()
            raise Exception("Something went wrong when saving changes to the database") from e

    async def dispose(self) -> None:
        if self._session:
            await self._session.close()
        await self._engine.dispose()

    async def __aenter__(self) -> Self:
        async with self._engine.begin() as conn:
            await conn.run_sync(self._metadata.create_all, checkfirst=True)

        self._session = self._session_factory()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        if exc_type is None:
            await self.save_changes()
        else:
            await self._get_session().rollback()
        await self._get_session().close()
