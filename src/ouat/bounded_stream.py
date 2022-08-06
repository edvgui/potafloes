import asyncio
from typing import (Any, Awaitable, Callable, Generator, Optional, Set, Type,
                    TypeVar)

from ouat.exceptions import (BoundedStreamOverflowException,
                             IncompleteBoundedStreamException)
from ouat.stream import STREAM_MARKER, Stream

X = TypeVar("X")
Y = TypeVar("Y")


class BoundedStream(Stream[X]):
    """
    A bounded stream object can be used to register callbacks, that will be called for
    each item that is sent into the stream.
    In addition to its superclass, this one has a notion of completion and upper bound.
    For a bounded stream to be considered complete, at least `min` elements and not more
    than `max` need to be to the stream.  Every time a None element is added, we keep it
    out of the stream be increase the counter.  We stop increasing the counter once we
    reach `max`, and check the stream size constraints then.
    """

    def __init__(
        self,
        bearer: Y,
        placeholder: str,
        object_type: Type[X],
        *,
        min: int = 0,
        max: int,
    ) -> None:
        super().__init__(bearer, placeholder, object_type)

        if min < 0:
            raise ValueError(f"Invalid lower bound: {min} < 0")

        if max < min:
            raise ValueError(f"Invalid upper bound: {max} < {min}")

        self._min = min
        self._max = max
        self._count = 0
        self._completed: Awaitable[Set[X]] = asyncio.Future(
            loop=self._context.event_loop()
        )

        self._context.register(self._completed)

    def send(self, item: Optional[X]) -> None:
        """
        Similarly to its superclass, send an element in the stream.  We additionally check
        here that the size of the stream is within the constraints (> min, < max).  Once the
        stream is completed, we complete the inner task, the bounded stream can be awaited
        and will return the full set of values.
        """
        if item not in self._items:
            # We increment the counter for each element we see that is not
            # yet in the stream, even if it is None
            self._count += 1

        if self._count < self._max:
            # The stream has not been called enough yet
            return super().send(item)

        if self._count > self._max:
            # The stream is already full
            raise BoundedStreamOverflowException(self, item)

        # Add the last item in the stream
        super().send(item)

        if len(self._items) < self._min:
            # The stream is supposed to be complete but doesn't have
            # enough elements
            raise IncompleteBoundedStreamException(self)

        self._completed.set_result(self._items)

    def __await__(self) -> Generator[Any, None, Set[X]]:
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
