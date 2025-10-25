from typing import Any
from dataclasses import dataclass

from .base import BaseEngineOptions


@dataclass(frozen=True)
class PostgresEngineOptions(BaseEngineOptions):
    server_side_cursors: bool = False
    use_native_unicode: bool = True
    client_encoding: str = "utf-8"

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
