import asyncio
import logging
from asyncio import Future
from typing import List, Optional

LOGGER = logging.getLogger(__name__)


class ContextNotReadyError(RuntimeError):
    """
    This exception is raised when Context.get() is called before
    the context is actually created.
    """


class Context:
    """
    The context object is responsible for holding the event loop in use
    and to keep track of all the tasks that needs to be completed before
    we can consider the state complete.

    This is used internally by the different framework components.  This
    should not be exposed directly to the end user.
    """

    __context: Optional["Context"] = None

    def __init__(self) -> None:
        self.tasks: List[asyncio.Task] = []
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

    def event_loop(
        self, *, loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> asyncio.AbstractEventLoop:
        if self._event_loop is not None:
            return self._event_loop

        if loop is None:
            LOGGER.warning("No event loop provided, using the default one")
            loop = asyncio.get_running_loop()

        self._event_loop = loop
        return self._event_loop

    def register(self, task: asyncio.Task) -> None:
        self.tasks.append(task)

    def gather(self) -> Future:
        return asyncio.gather(*self.tasks, return_exceptions=False)

    @classmethod
    def get(cls, *, create_ok: bool = False) -> "Context":
        if cls.__context is not None:
            return cls.__context

        if not create_ok:
            raise ContextNotReadyError(
                "Trying to access the context before it is created!"
            )

        cls.__context = Context()
        return cls.__context
