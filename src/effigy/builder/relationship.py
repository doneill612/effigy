from typing import Any, Callable, TypeVar, Generic, Type, TYPE_CHECKING, get_type_hints
from enum import Enum

from sqlalchemy.orm import InstrumentedAttribute, relationship

from effigy.entity import EntityProxy

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
        entity_config: "EntityConfiguration[T]",
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
        self._related_entity: type[object] | None = None

        self._cascade: str = "save-update, merge"
        self._lazy: str = "select"
        self._back_populates: str | None = None

    def with_foreign_key(
        self, navigation: Callable[[EntityProxy[object]], InstrumentedAttribute[object]]
    ) -> "RelationshipConfiguration[T]":
        hints = get_type_hints(self._entity_type)
        navtype = hints.get(self._navigation_name)

        if hasattr(navtype, "__args__"):
            self._related_entity = getattr(navtype, "__args__")[0]
        else:
            self._related_entity = navtype

        if self._related_entity:
            proxy = EntityProxy(self._related_entity)
            fkattr = navigation(proxy)
            self._fk_prop = fkattr.key

        return self

    def backpopulates(
        self, navigation: Callable[[EntityProxy[object]], InstrumentedAttribute[object]]
    ) -> "RelationshipConfiguration[T]":
        if not self._related_entity:
            raise ValueError("")
        proxy = EntityProxy(self._related_entity)
        backpop = navigation(proxy)
        self._inverse_prop = backpop.key
        self._back_populates = backpop.key
        return self

    def cascade(self, cascade: str) -> "RelationshipConfiguration[T]":
        self._cascade = cascade
        return self

    def with_lazy_loading(self, lazy: str = "select") -> "RelationshipConfiguration[T]":
        self._lazy = lazy
        return self

    def _apply(self) -> None:
        rel_kwargs: dict[str, Any] = {"cascade": self._cascade, "lazy": self._lazy}

        if self._back_populates:
            rel_kwargs["back_populates"] = self._back_populates

        if self._fk_prop and self._related_entity:
            # get the actual column reference from the related entity
            fk_attr = getattr(self._related_entity, self._fk_prop, None)
            if fk_attr is not None:
                rel_kwargs["foreign_keys"] = [fk_attr]

        rel = relationship(self._related_entity, **rel_kwargs)
        setattr(self._entity_type, self._navigation_name, rel)
