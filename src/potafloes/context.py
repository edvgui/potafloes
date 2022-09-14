from __future__ import annotations

import asyncio
import logging
import threading
import typing

from potafloes import exceptions

LOGGER = logging.getLogger(__name__)


class Context:
    """
    The context object is responsible for holding the event loop in use
    and to keep track of all the tasks that needs to be completed before
    we can consider the state complete.

    This is used internally by the different framework components.  This
    should not be exposed directly to the end user.
    """

    __contexts: dict[str, Context] = {}

    def __new__(cls: type[Context]) -> Context:
        new_instance = object.__new__(cls)
        new_instance.__init__()

        new_instance.logger.name = str(new_instance)

        if new_instance.name not in cls.__contexts:
            cls.__contexts[new_instance.name] = new_instance
            new_instance.logger.debug("New context created")

        return cls.__contexts[new_instance.name]

    def __init__(self) -> None:
        self.name = threading.current_thread().name
        self.tasks: list[asyncio.Task] = []

        self._initialized: bool = False
        self._frozen: bool = False
        self._finalizer: asyncio.Future | None = None

        self.logger = logging.getLogger(__name__)

    @property
    def initalized(self) -> bool:
        return self._initialized

    @property
    def frozen(self) -> bool:
        return self._frozen

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    def register(self, task: asyncio.Task) -> None:
        self.tasks.append(task)

    def init(self) -> None:
        # First, we make sure that the context is not already initialized
        if self.initalized:
            raise exceptions.ContextAlreadyInitializedException(
                "This context is already initialized"
            )

        self._initialized = True

        # Finally, we setup the exception handler for our loop
        def handle_exception(loop: asyncio.AbstractEventLoop, context: dict) -> None:
            msg = context.get("exception", context["message"])
            self.logger.error(f"Caught exception: {msg}")
            self.stop(force=True)

        self.event_loop.set_exception_handler(handle_exception)

    def _finalizer_callback(self, finalizer: asyncio.Future) -> None:
        if self.tasks:
            # We still have some pending tasks
            self.finalize()
        else:
            self.freeze()

    def finalize(self) -> None:
        self.logger.debug(f"Finalizing context with {len(self.tasks)} pending tasks")
        if self.tasks:
            self._finalizer = asyncio.gather(*self.tasks, return_exceptions=False)
            self._finalizer.add_done_callback(self._finalizer_callback)
            self.tasks.clear()

    def freeze(self) -> None:
        # First we make sure that this context is not frozen yet
        if self.frozen:
            raise exceptions.ContextAlreadyFrozenException(
                "This context is already frozen"
            )

        self._frozen = True

        # Then we freeze all the entity context related to this context
        from potafloes import entity_context, entity_type

        for et in entity_type.ENTITY_TYPES:
            ec = entity_context.EntityContext[object].get(entity_type=et, context=self)
            ec.freeze()

    def stop(self, *, force: bool = False) -> asyncio.Future:
        if force:
            self.logger.debug("Stopping loop")
            for task in self.tasks:
                task.cancel("Task cancelled by context shutdown")

        if self._finalizer is None:
            self.finalize()

        assert self._finalizer is not None  # Make mypy happy
        return self._finalizer

    def reset(self) -> None:
        """
        Get all the instances of all the entities it can find, and delete them.
        """
        from potafloes import entity_context, entity_type

        for et in entity_type.ENTITY_TYPES:
            ec = entity_context.EntityContext[object].get(entity_type=et, context=self)
            ec.reset()

        self._finalizer = None
        self._initialized = False
        self._frozen = False

    def run(
        self,
        entrypoint: typing.Callable[[], typing.Coroutine[typing.Any, typing.Any, None]],
    ) -> None:
        async def run() -> None:
            self.init()
            self.register(self.event_loop.create_task(entrypoint(), name="main"))
            await self.stop()

        asyncio.run(run())

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"