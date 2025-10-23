from typing import Protocol, Any


class DatabaseProvider(Protocol):
    def get_connection_string(self) -> str:
        """Gets the complete SQLAlchemy compatible connection string for the database.

        Format: `dialect+driver://username:password@host:port/database`

        Returns:
            A database connection string
        """
        ...

    # TODO: probably want to come up with a type instead of dict[str, Any]
    def get_engine_options(self) -> dict[str, Any]:
        """Gets SQLAlchemy engine configuration options."""
        ...
