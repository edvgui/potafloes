
from asyncio import Future, Task
import asyncio
from typing import Iterator, List


class TaskManager:
    __tasks: List[Task] = []
    
    @staticmethod
    def register(task: Task) -> None:
        TaskManager.__tasks.append(task)
    
    @staticmethod
    def gather() -> Future:
        return asyncio.gather(*TaskManager.__tasks, return_exceptions=True)
