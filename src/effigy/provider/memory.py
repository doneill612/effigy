from typing import Any

from sqlalchemy.pool import StaticPool


class InMemoryProvider:
    """A simple in-memory database provider that leverages sqlite"""

    def __init__(self, use_async: bool = False):
        self._async = use_async

    def get_connection_string(self) -> str:
        if self._async:
            return "sqlite+aiosqlite:///:memory:"
        return "sqlite:///:memory:"

    def get_engine_options(self) -> dict[str, Any]:
        options: dict[str, Any] = {"poolclass": StaticPool}

        if not self._async:
            # allow multithreaded access
            options["connect_args"] = {"check_same_thread": False}
        return options
