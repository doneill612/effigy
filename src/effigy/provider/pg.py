from typing import Any
from pydantic import Field

from urllib.parse import quote_plus

from .base import BaseEngineOptions, DatabaseProvider


class PostgresEngineOptions(BaseEngineOptions):

    host: str = Field(...)
    port: int = Field(default=5432)
    database: str = Field(...)
    username: str = Field(...)
    password: str = Field(...)

    use_async: bool = Field(default=False)

    server_side_cursors: bool = Field(default=False)
    use_native_unicode: bool = Field(default=True)
    client_encoding: str = Field(default="utf-8")

    def to_engine_opts(self) -> dict[str, Any]:
        opts = super().to_engine_opts()

        connect_args = opts.get("connect_args", {})
        if connect_args:
            connect_args = connect_args.copy()
        if not self.use_native_unicode:
            connect_args["use_native_unicode"] = False
        if self.client_encoding != "utf-8":
            connect_args["client_encoding"] = self.client_encoding
        if connect_args:
            opts["connect_args"] = connect_args
        if self.server_side_cursors:
            opts["server_side_cursors"] = True

        return opts

class PostgresProvider(DatabaseProvider[PostgresEngineOptions]):

    def get_connection_string(self) -> str:

        driver = "asyncpg" if self._opt.use_async else "psycopg2"
        pw = quote_plus(self._opt.password) if self._opt.password else ""
        auth = f"{self._opt.username}:{pw}@" if pw else f"{self._opt.username}"

        return (
            f"postgresql+{driver}://{auth}{self._opt.host}:{self._opt.port}/{self._opt.database}"
        )

