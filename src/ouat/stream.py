import logging
from typing import (Any, Callable, Coroutine, Generic, List, Optional, Set,
                    Type, TypeVar, Union)

from ouat.context import Context
from ouat.exceptions import StreamItemTypeException

X = TypeVar("X")
Y = TypeVar("Y")


STREAM_MARKER = "entity_stream"


class Stream(Generic[X]):
    """
    A stream object can be used to register callbacks, that will be called for
    each item that is sent into the stream.  A stream has no upper bound, it
    is potentially infinite (only limited by memory of the system).  You should
    never await for this to finish, it doesn't even have a notion of completion.
    """

    def __init__(self, bearer: Y, placeholder: str, object_type: Type[X]) -> None:
        """
        :param bearer: The object this stream is attached to.
        :param placeholder: The name of the object function this stream is a placeholder for.
        :param object_type: The type of objects to expect in the stream.  Objects introduced
            in the stream are expected to be a subclass of this type.
        """
        self._bearer = bearer
        self._placeholder = placeholder
        self._object_type = object_type

        self._callbacks: List[Callable[[X], Coroutine[Any, Any, None]]] = []
        self._items: Set[X] = set()

        self._context = Context()
        self._logger = logging.getLogger(
            f"{type(self).__name__}@{self._bearer}.{self._placeholder}"
        )

    def _trigger_callback(
        self, callback: Callable[[X], Coroutine[Any, Any, None]], item: X
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

    def subscribe(self, callback: Callable[[X], Coroutine[Any, Any, None]]) -> None:
        """
        Subscribe to each item emitted in this stream.  The callback should be a coroutine
        and will be called exactly once for each item in the stream.
        """
        self._callbacks.append(callback)
        for item in self._items:
            self._trigger_callback(callback, item)

    def send(self, item: Optional[X]) -> None:
        """
        Send a new item in the stream.  All registered callback will be called with
        this item in argument, unless this item has already been sent or the item
        is None.
        """
        if item is None:
            # We skip None items
            return

        if item in self._items:
            # We skip items which are already in the set
            return

        if not isinstance(item, self._object_type):
            raise StreamItemTypeException(self, item)

        self._items.add(item)
        for callback in self._callbacks:
            self._trigger_callback(callback, item)

    def __iadd__(self, other: Optional[Union[X, "Stream[X]"]]) -> "Stream[X]":
        """
        When using the += operator, we expect the other element to be either a stream,
        in which case we will subscribe to it and add all its items to this stream, or
        an object, in which case we try to add it to the stream.
        """
        if isinstance(other, Stream):
            async def cb(item: X) -> None:
                self.send(item)

            other.subscribe(cb)
            return self

        self.send(other)
        return self


def stream(func: Callable[["Y"], Type[X]]) -> Callable[["Y"], Stream[X]]:
    """
    Mark the current method as a stream, on which will be added and potentially
    subscribed items
    """
    # This is the name of the function wearing the decorator
    stream_name = func.__name__

    # This is the name of the attribute we will add to the object to store the bounded stream
    response_attribute = f"__{stream_name}_stream"

    def stream_or_cache(self: "Y") -> Stream[X]:
        if not hasattr(self, response_attribute):
            # We set the attribute using the object __setattr__ method as our object
            # is frozen, normal setattr doesn't work
            object.__setattr__(
                self, response_attribute, Stream(self, stream_name, func(self))
            )

        existing_stream = getattr(self, response_attribute)
        return existing_stream

    stream_or_cache.__name__ = stream_name
    setattr(stream_or_cache, STREAM_MARKER, True)

    return stream_or_cache
