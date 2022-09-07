import logging
from typing import (Any, Callable, Coroutine, Dict, Generic, List, Sequence,
                    Type, TypeVar)

from patasync.exceptions import DomainModifiedAfterFreezeException

X = TypeVar("X")
Y = TypeVar("Y")


class EntityDomain(Generic[X]):
    """
    An entity domain instance will hold all the static information about an entity
    type:
     - The indices that can be used the query instances of the entity
     - The double binding that needs to be setup when creating new instances
     - The implementations that needs to be called for each instance of the class

    The domain can only be edited while loading the model.  It should never be
    unfrozen when a context becomes active.
    """

    __entity_domains: Dict[Type[Y], "EntityDomain[Y]"] = dict()

    def __init__(self, entity_type: Type[X]) -> None:
        super().__init__()
        self.entity_type = entity_type
        self.logger = logging.getLogger(f"{entity_type.__name__}Domain")

        self._indices: List[Callable[[X], X]] = list()
        self._implementations: List[Callable[[X], Coroutine[Any, Any, None]]] = list()

        self._frozen = False

    @property
    def frozen(self) -> bool:
        return self._frozen

    @property
    def indices(self) -> Sequence[Callable[[X], Y]]:
        if not self.frozen:
            self.logger.warning(
                "Accessing indices before the domain is frozen, list might be incomplete"
            )
        return self._indices

    @property
    def implementations(
        self,
    ) -> Sequence[Callable[[X], Coroutine[Any, Any, None]]]:
        if not self.frozen:
            self.logger.warning(
                "Accessing implementations before the domain is frozen, list might be incomplete"
            )
        return self._implementations

    def add_index(self, *, index: Callable[[X], Y]) -> None:
        if self.frozen:
            raise DomainModifiedAfterFreezeException(
                f"Can not add index {index} to domain, it is already frozen."
            )

        self.logger.debug("Register index %s", str(index))
        self._indices.append(index)

    def add_implementation(
        self, *, implementation: Callable[[X], Coroutine[Any, Any, None]]
    ) -> None:
        if self.frozen:
            raise DomainModifiedAfterFreezeException(
                f"Can not add implementation {implementation} to domain, it is already frozen."
            )

        self.logger.debug("Register implementation %s", str(implementation))
        self._implementations.append(implementation)

    def freeze(self) -> None:
        self.logger.info("Freezing domain")
        self._frozen = True

    @classmethod
    def get(cls: Type["EntityDomain[X]"], *, entity_type: Type[X]) -> "EntityDomain[X]":
        """
        Get the domain instance for this entity type.
        """
        if entity_type not in cls.__entity_domains:
            cls.__entity_domains[entity_type] = EntityDomain(entity_type)

        return cls.__entity_domains[entity_type]
