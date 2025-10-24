from abc import ABC, abstractmethod
from typing import ClassVar, get_origin, get_args, Any
from typing_extensions import Self

from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session

from .builder.core import DbBuilder
from .dbset import AsyncDbSet, DbSet
from .configuration import DbContextConfiguration
from .provider.base import DatabaseProvider


class DbContext(ABC):
    """Synchronous database context"""

    _configuration: ClassVar[DbContextConfiguration | None] = None

    def __init__(self, provider: DatabaseProvider, **engine_options: dict[str, Any]):
        connection_string = provider.get_connection_string()
        opts = provider.get_engine_options()

        opts = {**opts, **engine_options}
        self._engine = create_engine(connection_string, **opts)
        self._session_factory = sessionmaker(bind=self._engine)
        self._session: Session | None = None
        self._init_dbsets()

    @abstractmethod
    def setup(self, builder: DbBuilder) -> None: ...

    @property
    def session(self) -> Session:
        if not self._session:
            self._session = self._session_factory()
        return self._session

    @classmethod
    def configure(cls) -> DbContextConfiguration:
        config = DbContextConfiguration()
        cls._configuration = config
        return config

    @classmethod
    def create(
        cls, *, provider: DatabaseProvider | None = None, **engine_opts: dict[str, Any]
    ) -> Self:
        final_provider = provider
        final_opts = {**engine_opts}
        if cls._configuration:
            config = cls._configuration.build()
            if final_provider is None:
                final_provider = config["provider"]
            final_opts = {**config["engine_opts"], **final_opts}

        if final_provider is None:
            raise ValueError(f"No database provider configured for {cls.__name__}.")
        return cls(final_provider, **final_opts)

    def _init_dbsets(self) -> None:
        metadata = MetaData()
        builder = DbBuilder(metadata)

        self.setup(builder)

        builder.finalize()

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
            self.session.flush()
            # capture counts before commit clears them
            change_count = (
                len(self.session.dirty) + len(self.session.new) + len(self.session.deleted)
            )
            self.session.commit()
            return change_count
        except Exception as e:
            self.session.rollback()
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
            self.session.rollback()
        self.session.close()


class AsyncDbContext(ABC):
    """Asynchronous database context"""

    _configuration: ClassVar[DbContextConfiguration | None] = None

    def __init__(self, provider: DatabaseProvider, **engine_options: dict[str, Any]):
        connection_string = provider.get_connection_string()
        opts = provider.get_engine_options()

        opts = {**opts, **engine_options}

        self._engine = create_async_engine(connection_string, **opts)
        self._session_factory = async_sessionmaker(bind=self._engine, class_=AsyncSession)
        self._session: AsyncSession | None = None
        self._init_dbsets()

    @property
    def session(self) -> AsyncSession:
        if not self._session:
            raise RuntimeError("Session not initialized. Use `async with`.")
        return self._session

    @abstractmethod
    def setup(self, builder: DbBuilder) -> None: ...

    @classmethod
    def configure(cls) -> DbContextConfiguration:
        config = DbContextConfiguration()
        cls._configuration = config
        return config

    @classmethod
    def create(
        cls, *, provider: DatabaseProvider | None = None, **engine_opts: dict[str, Any]
    ) -> Self:
        final_provider = provider
        final_opts = {**engine_opts}
        if cls._configuration:
            config = cls._configuration.build()
            if final_provider is None:
                final_provider = config["provider"]
            final_opts = {**config["engine_opts"], **final_opts}

        if final_provider is None:
            raise ValueError(f"No database provider configured for {cls.__name__}.")
        return cls(final_provider, **final_opts)

    def _init_dbsets(self) -> None:
        metadata = MetaData()
        builder = DbBuilder(metadata)

        self.setup(builder)

        builder.finalize()

        for name, annotation in self.__annotations__.items():
            if get_origin(annotation) is AsyncDbSet:
                entity_type = get_args(annotation)[0]
                dbset = AsyncDbSet(entity_type, self)
                setattr(self, name, dbset)

    async def save_changes(self) -> int:
        """Attempts to persist changes to the database.

        Returns:
            The total number of tracked changes in this operation
        """
        try:
            await self.session.flush()
            # capture counts before commit clears them
            change_count = (
                len(self.session.dirty) + len(self.session.new) + len(self.session.deleted)
            )
            await self.session.commit()
            return change_count
        except Exception as e:
            await self.session.rollback()
            raise Exception("Something went wrong when saving changes to the database") from e

    async def dispose(self) -> None:
        if self._session:
            await self._session.close()
        await self._engine.dispose()

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        if exc_type is None:
            await self.save_changes()
        else:
            await self.session.rollback()
        await self.session.close()
