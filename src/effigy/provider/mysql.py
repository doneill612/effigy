from typing import Any

from dataclasses import dataclass

from .base import BaseEngineOptions


@dataclass(frozen=True)
class MySqlEngineOptions(BaseEngineOptions):
    charset: str = "utf8mb4"
    use_unicode: bool = True

    def to_engine_opts(self) -> dict[str, Any]:
        opts = super().to_engine_opts()

        connect_args = opts.get("connect_args", {})
        if connect_args:
            connect_args = connect_args.copy()

        connect_args["charset"] = self.charset
        connect_args["use_unicode"] = self.use_unicode
        opts["connect_args"] = connect_args
        return opts
