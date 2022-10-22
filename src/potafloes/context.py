from __future__ import annotations

import asyncio
import logging
import threading
import typing

from potafloes import exceptions


class Context:
    """
    The context object is responsible for holding the event loop in use
    and to keep track of all the tasks that needs to be completed before
    we can consider the state complete.

    This is used internally by the different framework components.  This
    should not be exposed directly to the end user.
    """

    __contexts: dict[str, Context] = {}

    def __init__(self, name: str) -> None:
        self.name = name
        self.tasks: list[asyncio.Task[object] | asyncio.Future[object]] = []

        self._initialized: bool = False
        self._frozen: bool = False
        self._finalizer: asyncio.Future[None] | None = None
        self._errors: list[Exception] = []

        self.logger = logging.getLogger(str(self))
        self.logger.debug("New context created")

    @property
    def initialized(self) -> bool:
        return self._initialized

    @property
    def frozen(self) -> bool:
        return self._frozen

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    def register(self, task: asyncio.Task[typing.Any] | asyncio.Future[typing.Any]) -> None:
        self.tasks.append(task)

    def init(self) -> None:
        # First, we make sure that the context is not already initialized
        if self.initialized:
            raise exceptions.ContextAlreadyInitializedException("This context is already initialized")

        self._initialized = True

        # Finally, we setup the exception handler for our loop
        def handle_exception(loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
            exception = context.get("exception")

            if isinstance(exception, asyncio.CancelledError):
                # A task got cancelled
                pass
            elif exception is not None:
                assert isinstance(exception, Exception), f"{type(exception)}: {exception}"
                self._errors.append(exception)
            else:
                self._errors.append(RuntimeError(context["message"]))

            self.stop(force=True)

        self.event_loop.set_exception_handler(handle_exception)

    def finalize(self, *_: object) -> None:
        self.logger.debug(f"Finalizing context with {len(self.tasks)} pending tasks")
        if self.tasks:
            tasks = self.tasks
            self.tasks = []
            waiter = asyncio.gather(*tasks, return_exceptions=False)
            waiter.add_done_callback(self.finalize)
        else:
            self.freeze()

    def freeze(self) -> None:
        # First we make sure that this context is not frozen yet
        if self.frozen:
            raise exceptions.ContextAlreadyFrozenException("This context is already frozen")

        self._frozen = True
        self.logger.debug("Event loop completed")

        if self._finalizer is None:
            raise RuntimeError("Can not freeze a context that has not been stopped")

        self._finalizer.set_result(None)

        # Then we freeze all the entity context related to this context
        from potafloes import entity_context, entity_type

        for et in entity_type.ENTITY_TYPES:
            ec = entity_context.EntityContext[object].get(entity_type=et, context=self)
            ec.freeze()

    def stop(self, *, force: bool = False) -> asyncio.Future[None]:
        if force:
            self.logger.debug("Stopping loop")
            for task in self.tasks:
                task.cancel("Task cancelled by context shutdown")

        if self._finalizer is None:
            self._finalizer = asyncio.Future()
            self.finalize()

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

        if self._errors:
            raise self._errors[0]

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"

    @classmethod
    def get(cls) -> Context:
        name = threading.current_thread().name
        if name not in cls.__contexts:
            cls.__contexts[name] = Context(name)

        return cls.__contexts[name]
