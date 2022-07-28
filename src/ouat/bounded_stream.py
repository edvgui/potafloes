import asyncio
from typing import Any, Awaitable, Generator, Optional, Sequence, Set, Type, TypeVar
from ouat.task_manager import TaskManager
from ouat.stream import Stream


X = TypeVar("X")


class BoundedStream(Stream[X]):

    def __init__(self, object_type: Type[X], *, min: int = 0, max: int) -> None:
        super().__init__(object_type)

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
        if item not in self.items:
            print(f"New item ({self.count + 1}/{self.max}) in set: {item}")
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

        if len(self.items) < self.min:
            # The stream is supposed to be complete but doesn't have
            # enough elements
            raise RuntimeError(
                f"The stream is complete but doesn't have enough elements: {len(self.items)} < {self.min} "
                f"({self.items})"
            )

        self.completed.set_result(self.items)

    def __await__(self) -> Generator[Any, None, Set[X]]:
        return self.completed.__await__()
