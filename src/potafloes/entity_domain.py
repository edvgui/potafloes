import logging
import typing

from potafloes import attachment, exceptions

X = typing.TypeVar("X")
Y = typing.TypeVar("Y")


class EntityDomain(typing.Generic[X]):
    """
    An entity domain instance will hold all the static information about an entity
    type:
     - The indices that can be used the query instances of the entity
     - The double binding that needs to be setup when creating new instances
     - The implementations that needs to be called for each instance of the class

    The domain can only be edited while loading the model.  It should never be
    unfrozen when a context becomes active.
    """

    __entity_domains: typing.Dict[typing.Type, "EntityDomain"] = dict()

    def __init__(self, entity_type: typing.Type[X]) -> None:
        super().__init__()
        self.entity_type = entity_type
        self.logger = logging.getLogger(f"{entity_type.__name__}Domain")

        self._indices: typing.List[typing.Callable[[X], object]] = list()
        self._implementations: typing.List[
            typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]
        ] = list()
        self._attachments: typing.List[attachment.AttachmentReference] = list()

        self._frozen = False

    @property
    def frozen(self) -> bool:
        return self._frozen

    @property
    def indices(self) -> typing.Sequence[typing.Callable[[X], object]]:
        if not self.frozen:
            self.logger.warning(
                "Accessing indices before the domain is frozen, list might be incomplete"
            )
        return self._indices

    @property
    def implementations(
        self,
    ) -> typing.Sequence[
        typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]
    ]:
        if not self.frozen:
            self.logger.warning(
                "Accessing implementations before the domain is frozen, list might be incomplete"
            )
        return self._implementations

    @property
    def attachments(self) -> typing.Sequence[attachment.AttachmentReference]:
        if not self.frozen:
            self.logger.warning(
                "Accessing attachments before the domain is frozen, list might be incomplete"
            )
        return self._attachments

    def add_index(self, *, index: typing.Callable[[X], Y]) -> None:
        if self.frozen:
            raise exceptions.DomainModifiedAfterFreezeException(
                f"Can not add index {index} to domain, it is already frozen."
            )

        self.logger.debug("Register index %s", str(index))
        self._indices.append(index)

    def add_implementation(
        self,
        *,
        implementation: typing.Callable[
            [X], typing.Coroutine[typing.Any, typing.Any, None]
        ],
    ) -> None:
        if self.frozen:
            raise exceptions.DomainModifiedAfterFreezeException(
                f"Can not add implementation {implementation} to domain, it is already frozen."
            )

        self.logger.debug("Register implementation %s", str(implementation))
        self._implementations.append(implementation)

    def add_attachment(
        self, *, attachment_reference: attachment.AttachmentReference
    ) -> None:
        if self.frozen:
            raise exceptions.DomainModifiedAfterFreezeException(
                f"Can not add attachment {attachment_reference} to domain, it is already frozen."
            )

        self.logger.debug("Registering attachment %s", str(attachment_reference))
        self._attachments.append(attachment_reference)

    def freeze(self) -> None:
        self.logger.info("Freezing domain")
        self._frozen = True

    @classmethod
    def get(
        cls: typing.Type["EntityDomain[X]"], *, entity_type: typing.Type[X]
    ) -> "EntityDomain[X]":
        """
        Get the domain instance for this entity type.
        """
        if entity_type not in cls.__entity_domains:
            cls.__entity_domains[entity_type] = EntityDomain(entity_type)

        return cls.__entity_domains[entity_type]
