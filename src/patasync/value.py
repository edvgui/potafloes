import asyncio
import typing
from patasync import attachment

from patasync.exceptions import (BoundedStreamOverflowException, DoubleSetException,
                                 IncompleteBoundedStreamException)
from patasync.stream import STREAM_MARKER, Stream

X = typing.TypeVar("X")
Y = typing.TypeVar("Y")


class Value(attachment.Attachment[X]):
    """
    """

    def __init__(
        self,
        bearer: Y,
        placeholder: str,
        object_type: typing.Type[X],
        *,
        optional: bool,
    ) -> None:
        super().__init__(bearer, placeholder, object_type)
        self._optional = optional
        self._completed: typing.Awaitable[X] = asyncio.Future(
            loop=self._context.event_loop
        )

        self._context.register(self._completed)

    def subscribe(self, callback: typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]) -> None:
        def trigger_callback_if_not_none(item: typing.Optional[X]):
            if item is not None:
                self._trigger_callback(callback)

        self._completed.add_done_callback(trigger_callback_if_not_none)

    def send(self, item: typing.Optional[X]) -> None:
        if item is None and not self._optional:
            raise ValueError("")  # TODO

        if self._completed.done():
            if self._completed.result() is item:
                return
            
            raise DoubleSetException(self._bearer, self._bearer, self._placeholder, None)  # TODO

        self._completed.set_result(item)

    def __await__(self) -> typing.Generator[typing.Any, None, X]:
        return self._completed.__await__()


def bounded_stream(
    *, min: int = 0, max: int
) -> Callable[[Callable[["Y"], Type[X]]], Callable[["Y"], BoundedStream[X]]]:
    """
    Mark the current method as a bounded stream, on which will be added and potentially
    subscribed items.  This stream has a minimum and maximum size.
    """

    def bounded_stream_input(
        func: Callable[["Y"], Type[X]]
    ) -> Callable[["Y"], BoundedStream[X]]:

        # This is the name of the function wearing the decorator
        stream_name = func.__name__

        # This is the name of the attribute we will add to the object to store the bounded stream
        response_attribute = f"__{stream_name}_bounded_stream"

        def stream_or_cache(self: "Y") -> BoundedStream[X]:
            if not hasattr(self, response_attribute):
                # We set the attribute using the object __setattr__ method as our object
                # is frozen, normal setattr doesn't work
                object.__setattr__(
                    self,
                    response_attribute,
                    BoundedStream(self, stream_name, func(self), min=min, max=max),
                )

            existing_stream = getattr(self, response_attribute)
            assert isinstance(existing_stream, BoundedStream), type(existing_stream)
            return existing_stream

        stream_or_cache.__name__ = stream_name
        setattr(stream_or_cache, STREAM_MARKER, True)

        return stream_or_cache

    return bounded_stream_input
