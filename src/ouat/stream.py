import asyncio
from typing import Any, Callable, Coroutine, Generic, List, Optional, Set, Type, TypeVar
from ouat.task_manager import TaskManager


X = TypeVar("X")


class Stream(Generic[X]):
    """
    A stream object can be used to register callbacks, that will be called for
    each item that is sent into the stream.  A stream has no upper bound, it
    is potentially infinite (only limited by memory of the system).  You should
    never await for this to finish, it doesn't even have a notion of completion.
    """

    def __init__(self, object_type: Type[X]) -> None:
        self.object_type = object_type
        self.callbacks: List[Callable[[X], Coroutine[Any, Any, None]]] = []
        self.items: Set[X] = set()

    def subscribe(self, callback: Callable[[X], Coroutine[Any, Any, None]]) -> None:
        loop = asyncio.get_running_loop()

        self.callbacks.append(callback)
        for item in self.items:
            print(f"Calling {callback} on {item} (1)")
            to_be_awaited = callback(item)
            TaskManager.register(loop.create_task(to_be_awaited))
    
    def send(self, item: Optional[X]) -> None:
        if item is None:
            # We skip None items
            return

        if item in self.items:
            # We skip items which are already in the set
            return

        if not isinstance(item, self.object_type):
            raise ValueError(f"{item} should be of type {self.object_type} but isn't ({type(item)})")

        loop = asyncio.get_running_loop()

        self.items.add(item)
        for callback in self.callbacks:
            print(f"Calling {callback} on {item} (2)")
            to_be_awaited = callback(item)
            TaskManager.register(loop.create_task(to_be_awaited))
