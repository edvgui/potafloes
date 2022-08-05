import asyncio
from typing import Any, Callable, Coroutine, Generic, List, Optional, Set, Type, TypeVar
from ouat.task_manager import TaskManager


X = TypeVar("X")
Y = TypeVar("Y")


class Stream(Generic[X]):
    """
    A stream object can be used to register callbacks, that will be called for
    each item that is sent into the stream.  A stream has no upper bound, it
    is potentially infinite (only limited by memory of the system).  You should
    never await for this to finish, it doesn't even have a notion of completion.
    """

    def __init__(self, bearer: Y, placeholder: str, object_type: Type[X]) -> None:
        self._bearer = bearer
        self._placeholder = placeholder
        self._object_type = object_type

        self._callbacks: List[Callable[[X], Coroutine[Any, Any, None]]] = []
        self._items: Set[X] = set()

    def subscribe(self, callback: Callable[[X], Coroutine[Any, Any, None]]) -> None:
        loop = asyncio.get_running_loop()

        self._callbacks.append(callback)
        for item in self._items:
            print(f"Calling {callback} on {item} (1)")
            to_be_awaited = callback(item)
            TaskManager.register(loop.create_task(to_be_awaited))
    
    def send(self, item: Optional[X]) -> None:
        if item is None:
            # We skip None items
            print("Skipping None item")
            return

        if item in self._items:
            # We skip items which are already in the set
            print(f"{item} is already in {self._items}")
            return

        if not isinstance(item, self._object_type):
            raise ValueError(f"{item} should be of type {self._object_type} but isn't ({type(item)})")

        print(f"Adding {item} ({self._object_type}) to {self._bearer}.{self._placeholder}")

        loop = asyncio.get_running_loop()

        self._items.add(item)
        for callback in self._callbacks:
            print(f"Calling {callback} on {item} (2)")
            to_be_awaited = callback(item)
            TaskManager.register(loop.create_task(to_be_awaited))


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
            object.__setattr__(self, response_attribute, Stream(self, stream_name, func(self)))

        existing_stream = getattr(self, response_attribute)
        return existing_stream

    stream_or_cache.__name__ = stream_name
    
    return stream_or_cache
