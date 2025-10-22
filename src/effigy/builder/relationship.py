from typing import Literal, TypeVar, Generic, Type, TYPE_CHECKING
from enum import Enum

if TYPE_CHECKING:
    from .core import EntityConfiguration


T = TypeVar("T")


class RelationshipType(str, Enum):
    ONE_TO_MANY = "otm"
    MANY_TO_ONE = "mto"
    MANY_TO_MANY = "mtm"


class RelationshipConfiguration(Generic[T]):
    def __init__(
        self,
        navigation_name: str,
        relationship_type: RelationshipType | str,
        entity_type: Type[T],
        entity_config: EntityConfiguration[T],
    ):
        self._navigation_name = navigation_name
        self._relationship_type = (
            relationship_type
            if isinstance(relationship_type, RelationshipType)
            else RelationshipType(relationship_type)
        )
        self._entity_type = entity_type
        self._entity_config = entity_config

        self._fk_prop: str | None = None
        self._fk_col: str | None = None

        self._inverse_prop: str | None = None
        self._realted_entity: Type | None = None

        self._cascade: str = "save-update, merge"
        self._lazy: str = "select"
        self._back_populates: str | None = None
