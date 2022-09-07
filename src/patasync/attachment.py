import abc
import logging
import typing

from patasync import context

X = typing.TypeVar("X")
Y = typing.TypeVar("Y")


class Attachment(typing.Generic[X]):
    """
    An attachment represent a set of data that is attached to an entity, but doesn't
    have to be known at entity creation.  It can be later completed, while executing
    the model.
    """

    def __init__(self, bearer: Y, placeholder: str, object_type: typing.Type[X]) -> None:
        """
        :param bearer: The object this attachment is attached to.
        :param placeholder: The name of the object function this attachment is a placeholder for.
        :param object_type: The type of objects to expect in the attachment.  Objects introduced
            in the attachment are expected to be a subclass of this type.
        """
        self._bearer = bearer
        self._placeholder = placeholder
        self._object_type = object_type

        self._callbacks: typing.List[typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]] = []

        self._context = context.Context()
        self._logger = logging.getLogger(
            f"{type(self).__name__}@{self._bearer}.{self._placeholder}"
        )

    def _trigger_callback(
        self, callback: typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]], item: X
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
    def subscribe(self, callback: typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]) -> None:
        """
        Subscribe to the value set in this attachment.
        """

    @abc.abstractmethod
    def send(self, item: typing.Optional[X]) -> None:
        """
        Send an item and assign it to this attachment
        """
