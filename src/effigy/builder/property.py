from typing import Generic, Type, TypeVar, Any, Callable, TYPE_CHECKING
from typing_extensions import Self


if TYPE_CHECKING:
    from .core import EntityConfiguration


T = TypeVar("T")


class PropertyConfiguration(Generic[T]):
    def __init__(
        self,
        property_name: str,
        entity_type: Type[T],
        entity_configuration: "EntityConfiguration[T]",
    ):
        self._property_name = property_name
        self._entity_type = entity_type
        self._entity_configuration = entity_configuration
        self._required: bool = False
        self._autoincrement: bool = False
        self._max_length: int | None = None
        self._default: Any | None = None
        self._server_default: Any | None = None
        self._unique: bool = False
        self._validators: list[Callable[[Any], bool]] = []

    @property
    def is_unique(self) -> bool:
        return self._unique

    @property
    def is_autoincrement(self) -> bool:
        return self._autoincrement

    @property
    def default(self) -> Any | None:
        return self._default

    @property
    def server_default(self) -> Any | None:
        return self._server_default

    def property(self, navigation: Callable[[T], Any]) -> "PropertyConfiguration[T]":
        return self._entity_configuration.property(navigation)

    def required(self) -> Self:
        self._required = True
        return self

    def unique(self) -> Self:
        self._unique = True
        return self

    def max_len(self, max_len: int) -> Self:
        self._max_length = max_len
        return self

    def validate_with(self, validator: Callable[[Any], bool]) -> Self:
        self._validators.append(validator)
        return self

    def with_default(self, default: Any) -> Self:
        self._default = default
        return self

    def with_server_default(self, server_default: Any) -> Self:
        self._server_default = server_default
        return self

    def autoincrement(self) -> Self:
        self._autoincrement = True
        return self
