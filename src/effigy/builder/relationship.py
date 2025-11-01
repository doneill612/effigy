from typing import Any, Callable, TypeVar, Generic, Type, TYPE_CHECKING, cast
from typing_extensions import Self
from enum import Enum

from sqlalchemy import Table, Column, ForeignKey, MetaData
from sqlalchemy.orm import relationship

from ..entity import _EntityProxy

if TYPE_CHECKING:
    from .core import _EntityConfiguration


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
        entity_config: "_EntityConfiguration[T]",
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

        Note: This method is not applicable for MANY_TO_MANY relationships as they use
        association tables with auto-generated foreign keys.

        Args:
            navigation: Lambda that navigates to the FK property (e.g., lambda p: p.user_id)
        """
        if self._relationship_type == RelationshipType.MANY_TO_MANY:
            raise ValueError(
                "with_foreign_key() is not applicable for many-to-many relationships. "
                "Many-to-many relationships use auto-generated association tables."
            )

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
        else:
            # FK is on the current entity (the "many" side)
            fk_owner = self._entity_type

        if fk_owner:
            proxy = _EntityProxy(fk_owner)
            fkattr = navigation(proxy)
            self._fk_prop = fkattr.key

        return self

    def with_many(self, navigation: Callable[[Any], Any] | None = None) -> Self:
        """Converts a one-to-many relationship into a many-to-many relationship.

        This method follows the Entity Framework pattern: HasMany().WithMany()
        When called, it changes the relationship type from ONE_TO_MANY to MANY_TO_MANY
        and sets up the bidirectional inverse property if a navigation lambda is provided.

        Args:
            navigation: Optional lambda that navigates to the inverse property on the related
                       entity (e.g., lambda t: t.posts). If not provided, creates a unidirectional
                       many-to-many relationship.

        Returns:
            Self for method chaining

        Raises:
            ValueError: If called on a relationship that is not ONE_TO_MANY
        """
        if self._relationship_type != RelationshipType.ONE_TO_MANY:
            raise ValueError(
                f"with_many() can only be called on one-to-many relationships. "
                f"Current relationship type is {self._relationship_type.value}."
            )

        self._relationship_type = RelationshipType.MANY_TO_MANY

        # set up bidirectional relationship if navigation provided
        if navigation:
            if not self._related_entity:
                self._related_entity = self._determine_related_entity()
            proxy = _EntityProxy(self._related_entity)
            backpop = navigation(proxy)
            self._inverse_prop = backpop.key
            self._back_populates = backpop.key

        return self

    def backpopulates(self, navigation: Callable[[Any], Any]) -> Self:
        """Specifies the inverse property for bidirectional relationships.

        The lambda receives an instance-like proxy of the related entity and should
        navigate to the inverse relationship property.

        Args:
            navigation: Lambda that navigates to the inverse property (e.g., lambda u: u.posts)
        """
        if not self._related_entity:
            self._related_entity = self._determine_related_entity()
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

    def _determine_related_entity(self) -> Type[Any]:
        """Determines the related entity type from the navigation property annotation.

        This method inspects the type annotations to find the related entity type,
        handling both direct type references and forward references (strings).

        Returns:
            The related entity type

        Raises:
            ValueError: If the related entity cannot be determined
        """
        if not hasattr(self._entity_type, "__annotations__"):
            raise ValueError(
                f"Entity {self._entity_type.__name__} has no type annotations. "
                f"Cannot configure relationship for '{self._navigation_name}'."
            )

        entity_annotations = self._entity_type.__annotations__
        navtype = cast(Type[Any], entity_annotations.get(self._navigation_name))

        if navtype is None:
            raise ValueError(
                f"Navigation property '{self._navigation_name}' not found on entity "
                f"{self._entity_type.__name__}. Available properties: "
                f"{', '.join(entity_annotations.keys())}"
            )

        # extract the type from list[T] or similar generic types
        if hasattr(navtype, "__args__"):
            related = navtype.__args__[0]

            # handle forward reference strings
            if isinstance(related, str):
                import sys

                entity_module = sys.modules.get(self._entity_type.__module__)
                related_entity = getattr(entity_module, related, None) if entity_module else None

                # fail fast if forward reference can't be resolved
                if related_entity is None:
                    raise ValueError(
                        f"Cannot resolve forward reference '{related}' for relationship "
                        f"'{self._navigation_name}' on entity {self._entity_type.__name__}. "
                        f"Ensure the referenced entity '{related}' is defined at module level "
                        f"in {self._entity_type.__module__}."
                    )
                return cast(Type[Any], related_entity)
            else:
                return cast(Type[Any], related)
        else:
            return navtype

    def _create_association_table(self, metadata: MetaData) -> Table:
        """Creates an association table for many-to-many relationships.

        The table name is generated from both entity table names in alphabetical order.
        For example, Post and Tag entities would create a 'post_tag' association table.

        If the table already exists in the metadata (from a bidirectional relationship),
        it will be reused instead of creating a duplicate.

        Args:
            metadata: SQLAlchemy MetaData to attach the table to

        Returns:
            The created or existing association Table
        """
        if not self._related_entity:
            self._related_entity = self._determine_related_entity()

        current_table_name = getattr(self._entity_type, "__tablename__")
        related_table_name = getattr(self._related_entity, "__tablename__")

        table_names = sorted([current_table_name, related_table_name])
        association_table_name = f"{table_names[0]}_{table_names[1]}"

        if association_table_name in metadata.tables:
            return metadata.tables[association_table_name]

        current_table = getattr(self._entity_type, "__table__")
        related_table = getattr(self._related_entity, "__table__")

        current_pk_cols = [col for col in current_table.columns if col.primary_key]
        related_pk_cols = [col for col in related_table.columns if col.primary_key]

        if not current_pk_cols or not related_pk_cols:
            raise ValueError(
                f"Cannot create association table for many-to-many relationship. "
                f"Both entities must have primary keys defined."
            )

        current_fk_name = f"{current_table_name}_{current_pk_cols[0].name}"
        related_fk_name = f"{related_table_name}_{related_pk_cols[0].name}"

        association_table = Table(
            association_table_name,
            metadata,
            Column(
                current_fk_name,
                current_pk_cols[0].type,
                ForeignKey(f"{current_table_name}.{current_pk_cols[0].name}"),
                primary_key=True,
            ),
            Column(
                related_fk_name,
                related_pk_cols[0].type,
                ForeignKey(f"{related_table_name}.{related_pk_cols[0].name}"),
                primary_key=True,
            ),
        )

        return association_table

    def _apply(self) -> None:
        """Applies the relationship configuration by creating a SQLAlchemy relationship.

        For MANY_TO_MANY relationships, this creates an association table and uses
        the 'secondary' parameter. For other relationships, it uses foreign_keys.
        """
        rel_kwargs: dict[str, Any] = {"cascade": self._cascade, "lazy": self._lazy}

        if self._back_populates:
            rel_kwargs["back_populates"] = self._back_populates

        # handle many-to-many relationships (create association tables)
        if self._relationship_type == RelationshipType.MANY_TO_MANY:
            if not self._related_entity:
                self._related_entity = self._determine_related_entity()

            current_table = getattr(self._entity_type, "__table__")
            metadata = current_table.metadata

            association_table = self._create_association_table(metadata)
            rel_kwargs["secondary"] = association_table

        elif self._fk_prop and self._related_entity:
            # get the actual column reference from the related entity
            fk_attr = getattr(self._related_entity, self._fk_prop, None)
            if fk_attr is not None:
                rel_kwargs["foreign_keys"] = [fk_attr]

        rel = relationship(self._related_entity, **rel_kwargs)
        setattr(self._entity_type, self._navigation_name, rel)
