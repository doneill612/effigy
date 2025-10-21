from typing import Type, ClassVar, get_origin, get_args
from typing_extensions import Self

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session

from .configuration import DbContextConfiguration
from .provider.base import DatabaseProvider


class DbContext:
    """Synchronous database context"""

    _configuration: ClassVar[DbContextConfiguration | None] = None

    def __init__(self, provider: DatabaseProvider, **engine_options):
        connection_string = provider.get_connection_string()
        opts = provider.get_engine_options()

        opts = {**opts, **engine_options}
        self._engine = create_engine(connection_string, **opts)
        self._session_factory = sessionmaker(bind=self._engine)
        self._session: Session | None = None
        self._init_dbsets()

    # will come back to this once we start implementing the DB sets.
    def _init_dbsets(self) -> None: ...

    @property
    def session(self) -> Session:
        if not self._session:
            self._session = self._session_factory()
        return self._session

    def save_changes(self) -> int:
        """Attempts to persist changes to the database.

        Returns:
            The total number o ftracked changes after this operation
        """
        try:
            self.session.flush()
            self.session.commit()
            # return total number of tracked changes on this op
            return len(self.session.dirty) + len(self.session.new)
        except Exception as e:
            self.session.rollback()
            raise Exception("Something went wrong when saving changes to the database") from e

    def dispose(self) -> None:
        if self._session:
            self._session.close()
        self._engine.dispose()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.save_changes()
        else:
            self.session.rollback()
        self.session.close()


class AsyncDbContext: ...
