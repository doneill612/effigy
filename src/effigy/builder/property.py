from typing import Generic, Type, TypeVar, Any, Callable, TYPE_CHECKING

from sqlalchemy.orm import InstrumentedAttribute

from effigy.entity import EntityProxy

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
        self._max_length: int | None = None
        self._default: Any | None = None
        self._server_default: Any | None = None
        self._unique: bool = False
        self._validators: list[Callable[[Any], bool]] = []

    def property(
        self, navigation: Callable[[EntityProxy[T]], InstrumentedAttribute[object]]
    ) -> "PropertyConfiguration[T]":
        return self._entity_configuration.property(navigation)

    def required(self) -> "PropertyConfiguration[T]":
        self._required = True
        return self

    def unique(self) -> "PropertyConfiguration[T]":
        self._unique = True
        return self

    def max_len(self, max_len: int) -> "PropertyConfiguration[T]":
        self._max_length = max_len
        return self

    def validate_with(self, validator: Callable[[Any], bool]):
        self._validators.append(validator)
        return self

    def with_default(self, default: Any) -> "PropertyConfiguration[T]":
        self._default = default
        return self

    def with_server_default(self, server_default: Any) -> "PropertyConfiguration[T]":
        self._server_default = server_default
        return self
