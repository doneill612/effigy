from typing import Any
from pydantic import Field

from urllib.parse import quote_plus

from .base import BaseEngineOptions, DatabaseProvider


class MySqlEngineOptions(BaseEngineOptions):

    host: str = Field(...)
    port: int = Field(default=3306)
    database: str = Field(...)
    username: str = Field(...)
    password: str = Field(...)

    use_async: bool = Field(default=False)


    charset: str = Field(default="utf8mb4")
    use_unicode: bool = Field(default=True)

    def to_engine_opts(self) -> dict[str, Any]:
        opts = super().to_engine_opts()

        connect_args = opts.get("connect_args", {})
        if connect_args:
            connect_args = connect_args.copy()

        connect_args["charset"] = self.charset
        connect_args["use_unicode"] = self.use_unicode
        opts["connect_args"] = connect_args
        return opts

class MySqlProvider(DatabaseProvider[MySqlEngineOptions]):

    def get_connection_string(self) -> str:
        driver = "aiomysql" if self._opt.use_async else "pymysql"
        pw = quote_plus(self._opt.password) if self._opt.password else ""
        auth = f"{self._opt.username}:{pw}@" if pw else f"{self._opt.username}"

        return (
            f"mysql+{driver}://{auth}{self._opt.host}:{self._opt.port}/{self._opt.database}"
        ) 
