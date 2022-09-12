from __future__ import annotations

import abc
import dataclasses
import logging
import typing

from potafloes import context, exceptions

X = typing.TypeVar("X")
Y = typing.TypeVar("Y")

Callback = typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]


class Attachment(typing.Generic[X]):
    """
    An attachment represent a set of data that is attached to an entity, but doesn't
    have to be known at entity creation.  It can be later completed, while executing
    the model.
    """

    def __init__(self, bearer: object, placeholder: str, object_type: type[X]) -> None:
        """
        :param bearer: The object this attachment is attached to.
        :param placeholder: The name of the object function this attachment is a placeholder for.
        :param object_type: The type of objects to expect in the attachment.  Objects introduced
            in the attachment are expected to be a subclass of this type.
        """
        self._bearer = bearer
        self._placeholder = placeholder
        self._object_type = object_type

        self._callbacks: list[
            typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]
        ] = []

        self._context = context.Context()
        self._logger = logging.getLogger(
            f"{type(self).__name__}@{self._bearer}.{self._placeholder}"
        )

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
    def _insert(self, item: X | None) -> bool:
        """
        Insert this item into this attachment.  Returns True if the insertion
        was impacting for the attachment, False otherwise.
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

            callback = cb

        if callback is None:
            raise ValueError(
                "At least one of attachment or callback attribute should be set."
            )

        self._callbacks.append(callback)
        for item in self._all():
            self._trigger_callback(callback, item)

    def send(self, __item: X | None) -> None:
        """
        Send an item and assign it to this attachment
        """
        if __item is None:
            self._insert(None)
            return

        if not isinstance(__item, self._object_type):
            # Unexpected type for provided item, we raise an exception
            raise exceptions.AttachmentItemTypeException(self, __item)

        if self._insert(__item):
            # If the insertion changed something, we trigger all the known callback
            for callback in self._callbacks:
                self._trigger_callback(callback, __item)

    def __iadd__(self: A, other: X | Attachment[X] | None) -> A:
        """
        When using the += operator, we expect the other element to be either an attachment,
        in which case we will subscribe to it and add all its items to this attachment, or
        an object, in which case we try to add it to the attachment.
        """
        if isinstance(other, Attachment):
            other.subscribe(attachment=self)
            return self

        self.send(other)
        return self


@dataclasses.dataclass
class AttachmentReference(typing.Generic[X, Y]):
    """
    For internal use only.  Holds the information about the attachment that
    needs to be created when building the entity.
    """

    bearer_class: type[Y]
    placeholder: str
    inner_type_expression: str | None
    outer_type: type[Attachment]
    globals: dict
    locals: dict

    @property
    def inner_type(self) -> typing.Type:
        if self.inner_type_expression is None:
            return type(None)
        return eval(self.inner_type_expression, self.globals, self.locals)

    def attachment(self, bearer: Y) -> Attachment[X]:
        return self.outer_type(bearer, self.placeholder, self.inner_type)

    def validate(self, attachment: object) -> Attachment[X]:
        if not isinstance(attachment, self.outer_type):
            raise ValueError(
                f"Unexpected type for {self.placeholder}: "
                f"{type(attachment)} (expected {self.outer_type})"
            )

        if not isinstance(self.inner_type, typing.ForwardRef) and not issubclass(
            attachment._object_type, self.inner_type
        ):
            raise ValueError(
                f"Unexpected inner type for {self.placeholder}: "
                f"{type(attachment._object_type)} (expected {self.inner_type})"
            )

        return attachment


A = typing.TypeVar("A", bound=Attachment)
ATTACHMENT_TYPES: set[type[Attachment]] = set()


def attachment(attachment_class: type[A]) -> type[A]:
    """
    Use this decorator to register a new attachment type.  This type can then
    be used on any entity attribute and will automatically be built when the
    object is instantiated.
    """
    ATTACHMENT_TYPES.add(attachment_class)
    return attachment_class
