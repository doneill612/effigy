from typing import Any, Callable, TypeVar, Generic, Type, TYPE_CHECKING, get_type_hints
from typing_extensions import Self
from enum import Enum

from sqlalchemy.orm import relationship

from ..entity import _EntityProxy

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

    def with_foreign_key(self, navigation: Callable[[Any], Any]) -> Self:
        """Specifies the foreign key column for the relationship.

        The lambda receives an instance-like proxy of the FK owner entity and should
        navigate to the FK column:
        - For ONE_TO_MANY: FK is on the related entity (e.g., lambda p: p.user_id where p is Post)
        - FOR MANY_TO_ONE: FK is on the current entity (e.g., lambda p: p.user_id where p is Post)

        Args:
            navigation: Lambda that navigates to the FK property (e.g., lambda p: p.user_id)
        """
        if not hasattr(self._entity_type, "__annotations__"):
            raise ValueError(
                f"Entity {self._entity_type.__name__} has no type annotations. "
                f"Cannot configure relationship for '{self._navigation_name}'."
            )

        entity_annotations = self._entity_type.__annotations__
        navtype = entity_annotations.get(self._navigation_name)

        if navtype is None:
            raise ValueError(
                f"Navigation property '{self._navigation_name}' not found on entity "
                f"{self._entity_type.__name__}. Available properties: "
                f"{', '.join(entity_annotations.keys())}"
            )

        if hasattr(navtype, "__args__"):
            related = navtype.__args__[0]

            # handle forward reference strings
            if isinstance(related, str):
                import sys

                entity_module = sys.modules.get(self._entity_type.__module__)
                self._related_entity = (
                    getattr(entity_module, related, None) if entity_module else None
                )

                # fail fast if forward reference can't be resolved
                if self._related_entity is None:
                    raise ValueError(
                        f"Cannot resolve forward reference '{related}' for relationship "
                        f"'{self._navigation_name}' on entity {self._entity_type.__name__}. "
                        f"Ensure the referenced entity '{related}' is defined at module level "
                        f"in {self._entity_type.__module__}."
                    )
            else:
                self._related_entity = related
        else:
            self._related_entity = navtype

        if self._relationship_type == RelationshipType.ONE_TO_MANY:
            # FK is on the related entity (the "many" side)
            fk_owner = self._related_entity
        else:  # MANY_TO_ONE
            # FK is on the current entity (the "many" side)
            fk_owner = self._entity_type

        if fk_owner:
            proxy = _EntityProxy(fk_owner)
            fkattr = navigation(proxy)
            self._fk_prop = fkattr.key

        return self

    def backpopulates(self, navigation: Callable[[Any], Any]) -> Self:
        """Specifies the inverse property for bidirectional relationships.

        The lambda receives an instance-like proxy of the related entity and should
        navigate to the inverse relationship property.

        Args:
            navigation: Lambda that navigates to the inverse property (e.g., lambda u: u.posts)
        """
        if not self._related_entity:
            raise ValueError("Cannot call backpopulates before related entity is determined")
        proxy = _EntityProxy(self._related_entity)
        backpop = navigation(proxy)
        self._inverse_prop = backpop.key
        self._back_populates = backpop.key
        return self

    def cascade(self, cascade: str) -> Self:
        self._cascade = cascade
        return self

    def with_lazy_loading(self, lazy: str = "select") -> Self:
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
