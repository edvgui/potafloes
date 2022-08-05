import asyncio
from typing import Any, Awaitable, Callable, Generator, Optional, Sequence, Set, Type, TypeVar
from ouat.task_manager import TaskManager
from ouat.stream import Stream


X = TypeVar("X")
Y = TypeVar("Y")


class BoundedStream(Stream[X]):

    def __init__(self, bearer: Y, placeholder: str, object_type: Type[X], *, min: int = 0, max: int) -> None:
        super().__init__(bearer, placeholder, object_type)

        if min < 0:
            raise ValueError(f"Invalid lower bound: {min} < 0")

        if max < min:
            raise ValueError(f"Invalid upper bound: {max} < {min}")

        self.min = min
        self.max = max
        self.count = 0
        self.completed: Awaitable[Set[X]] = asyncio.Future()

        TaskManager.register(self.completed)
    
    def send(self, item: Optional[X]) -> None:
        if item not in self._items:
            self.count += 1

        if self.count < self.max:
            # The stream has not been called enough yet
            return super().send(item)

        if self.count > self.max:
            # The stream is already full
            raise RuntimeError(
                "The stream is already complete, no other element can be added to it"
            )

        # Add the last item in the stream
        super().send(item)

        if len(self._items) < self.min:
            # The stream is supposed to be complete but doesn't have
            # enough elements
            raise RuntimeError(
                f"The stream is complete but doesn't have enough elements: {len(self._items)} < {self.min} "
                f"({self._items})"
            )

        self.completed.set_result(self._items)

    def __await__(self) -> Generator[Any, None, Set[X]]:
        return self.completed.__await__()


def bounded_stream(*, min: int = 0, max: int) -> Callable[[Callable[["Y"], Type[X]]], Callable[["Y"], BoundedStream[X]]]:

    def bounded_stream_input(func: Callable[["Y"], Type[X]]) -> Callable[["Y"], BoundedStream[X]]:
        """
        Mark the current method as a stream, on which will be added and potentially
        subscribed items
        """

        # This is the name of the function wearing the decorator
        stream_name = func.__name__

        # This is the name of the attribute we will add to the object to store the bounded stream
        response_attribute = f"__{stream_name}_bounded_stream"

        def stream_or_cache(self: "Y") -> BoundedStream[X]:
            if not hasattr(self, response_attribute):
                # We set the attribute using the object __setattr__ method as our object
                # is frozen, normal setattr doesn't work
                object.__setattr__(self, response_attribute, BoundedStream(self, stream_name, func(self), min=min, max=max))

            existing_stream = getattr(self, response_attribute)
            assert isinstance(existing_stream, BoundedStream), type(existing_stream)
            return existing_stream

        stream_or_cache.__name__ = stream_name

        return stream_or_cache

    return bounded_stream_input
