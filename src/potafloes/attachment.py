from __future__ import annotations

import abc
import dataclasses
import logging
import types
import typing

from potafloes import context, definition, exceptions

X = typing.TypeVar("X")

Callback = typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]


class Attachment(typing.Generic[X]):
    """
    An attachment represent a set of data that is attached to an entity, but doesn't
    have to be known at entity creation.  It can be later completed, while executing
    the model.
    """

    def __init__(self, bearer: object, placeholder: str, object_type: type[X] | types.UnionType) -> None:
        """
        :param bearer: The object this attachment is attached to.
        :param placeholder: The name of the object function this attachment is a placeholder for.
        :param object_type: The type of objects to expect in the attachment.  Objects introduced
            in the attachment are expected to be a subclass of this type.
        """
        self._bearer = bearer
        self._placeholder = placeholder
        self._object_type = object_type

        self._callbacks: list[typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]] = []

        self._context = context.Context.get()
        self._logger = logging.getLogger(str(self))

    def _trigger_callback(
        self,
        callback: typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]],
        item: X,
    ) -> None:
        """
        Helper method to create a new task, which takes a coroutine and feeds
        it the item provided in parameter.
        """
        name = f"{callback.__name__}({item})"
        self._logger.debug("Starting new callback: %s", name)
        to_be_awaited = callback(item)
        self._context.register(
            self._context.event_loop.create_task(
                to_be_awaited,
                name=name,
            )
        )

    @abc.abstractmethod
    def _insert(self, item: X) -> None:
        """
        Insert this item into this attachment.  The item is guaranteed to not be present in _all()
        """

    @abc.abstractmethod
    def _all(self) -> typing.Iterable[X]:
        """
        Get all the items already received in this attachment.
        """

    @typing.overload
    def subscribe(self, *, attachment: Attachment[X]) -> None:
        """
        For each value added in this attachment, add them to the attachment in argument as well.
        """

    @typing.overload
    def subscribe(
        self,
        *,
        callback: Callback[X],
    ) -> None:
        """
        Subscribe to the value set in this attachment.
        """

    def subscribe(
        self,
        *,
        attachment: Attachment[X] | None = None,
        callback: Callback[X] | None = None,
    ) -> None:
        if attachment is not None:

            async def cb(item: X) -> None:
                assert attachment is not None  # Make mypy happy
                attachment.send(item)

            cb.__name__ = f"{self}_to_{attachment}"
            callback = cb

        if callback is None:
            raise ValueError("At least one of attachment or callback attribute should be set.")

        self._callbacks.append(callback)
        for item in self._all():
            self._trigger_callback(callback, item)

    def send(self, __item: X) -> None:
        """
        Send an item and assign it to this attachment
        """
        if not isinstance(__item, self._object_type):
            # Unexpected type for provided item, we raise an exception
            raise exceptions.AttachmentItemTypeException(self, __item)

        if __item not in self._all():
            # We only add the item if it is not yet included in the attachment
            # Then we trigger all the known callbacks
            self._insert(__item)
            for callback in self._callbacks:
                self._trigger_callback(callback, __item)
        else:
            self._logger.debug(f"{__item} is already in {[str(i) for i in self._all()]}")

    def __iadd__(self, other: X | Attachment[X]) -> Attachment[X]:
        """
        When using the += operator, we expect the other element to be either an attachment,
        in which case we will subscribe to it and add all its items to this attachment, or
        an object, in which case we try to add it to the attachment.
        """
        if isinstance(other, Attachment):
            if not issubclass(other._object_type, self._object_type):
                raise ValueError(f"Can not add items of type {other._object_type} to {self}")

            other.subscribe(attachment=self)
            return self

        self.send(other)
        return self

    def __str__(self) -> str:
        return f"{type(self).__name__}[{self._object_type}]@{self._bearer}.{self._placeholder}"


@dataclasses.dataclass
class AttachmentDefinition(definition.Definition):
    """
    For internal use only.  Holds the information about the attachment that
    needs to be created when building the entity.
    """

    outer_type: type[Attachment[object]]

    def inner_type(self) -> type | types.UnionType:
        if not hasattr(self._type, "__args__"):
            raise ValueError(f"Incomplete attachment type: {self.type_expression}")

        res = getattr(self._type, "__args__")[0]
        if isinstance(res, types.UnionType):
            return res

        assert isinstance(res, type), type(res)
        return res

    def attachment(self, bearer: object) -> Attachment[object]:
        inner_type = self.inner_type()
        return self.outer_type(bearer, self.placeholder, inner_type)

    def validate(self, attribute: object) -> object:
        """
        When validating an attachment assigned to an attachment placeholder, we verify:
        1. The the type of attachment is the same.
        2. The inner type of the attachment being attached is a subclass of the attachment
            defined in annotations.
        """
        if not isinstance(attribute, self.outer_type):
            raise ValueError(f"Unexpected type for {self.placeholder}: " f"{type(attribute)} (expected {self.outer_type})")

        inner_type = self.inner_type()
        if isinstance(attribute._object_type, types.UnionType):
            for attribute_inner_type in attribute._object_type.__args__:
                if not issubclass(attribute_inner_type, inner_type):
                    raise ValueError(
                        f"Unexpected inner type for {self.placeholder}: "
                        f"{type(attribute_inner_type)} (expected {inner_type})"
                    )
            return attribute

        if not issubclass(attribute._object_type, inner_type):
            raise ValueError(
                f"Unexpected inner type for {self.placeholder}: " f"{type(attribute._object_type)} (expected {inner_type})"
            )

        return attribute


A = typing.TypeVar("A", bound=Attachment[object])
Y = typing.TypeVar("Y", bound=object)
ATTACHMENT_TYPES: set[type[Attachment[typing.Any]]] = set()


def entity_attachment(attachment_class: type[Attachment[Y]]) -> type[Attachment[Y]]:
    """
    Use this decorator to register a new attachment type.  This type can then
    be used on any entity attribute and will automatically be built when the
    object is instantiated.
    """
    ATTACHMENT_TYPES.add(attachment_class)
    return attachment_class
