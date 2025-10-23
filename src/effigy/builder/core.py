from typing import Callable, Type, TypeVar, Generic

from sqlalchemy import MetaData
from sqlalchemy.orm import InstrumentedAttribute


from .index import IndexConfiguration
from .property import PropertyConfiguration
from .relationship import RelationshipConfiguration
from ..entity import EntityProxy

T = TypeVar("T")


class DbBuilder:
    """Fluent API for configuring effigy entities"""

    def __init__(self, metadata: MetaData):
        self._metadata = metadata
        self._entity_configs: dict[Type, EntityConfiguration] = {}

    def entity(self, entity_type: Type[T]) -> "EntityConfiguration[T]":
        if entity_type not in self._entity_configs:
            self._entity_configs[entity_type] = EntityConfiguration(entity_type, self)
        return self._entity_configs[entity_type]

    def finalize(self) -> None:
        for config in self._entity_configs.values():
            config._create_table(self._metadata)
        for config in self._entity_configs.values():
            config._create_relationships()


class EntityConfiguration(Generic[T]):

    def __init__(self, entity_type: type[T], builder: DbBuilder):
        self._entity_type = entity_type
        self._builder = builder
        self._table_name: str | None = None
        self._pks: list[str] = []
        self._properties: dict[str, PropertyConfiguration] = {}
        self._relationships: list[RelationshipConfiguration] = []
        self._indexes: list[IndexConfiguration] = []

    def property(
        self, navigation: Callable[[EntityProxy[T]], InstrumentedAttribute[object]]
    ) -> PropertyConfiguration[T]:
        proxy = EntityProxy(self._entity_type)
        prop_attr = navigation(proxy)
        prop_name = prop_attr.key

        if prop_name not in self._properties:
            self._properties[prop_name] = PropertyConfiguration(prop_name, self._entity_type, self)

        return self._properties[prop_name]

    def has_key(
        self, navigation: Callable[[EntityProxy[T]], InstrumentedAttribute[object]]
    ) -> "EntityConfiguration[T]":
        proxy = EntityProxy(self._entity_type)
        key_attr = navigation(proxy)
        key_name = key_attr.key

        self._pks.append(key_name)
        return self

    def _create_table(self, metadata: MetaData) -> None: ...

    def _create_relationships(self) -> None: ...
