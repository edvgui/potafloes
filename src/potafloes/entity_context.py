from __future__ import annotations

import asyncio
import logging
import typing

from potafloes.context import Context
from potafloes.exceptions import ContextModifiedAfterFreezeException

X = typing.TypeVar("X")
Y = typing.TypeVar("Y")


class EntityContext(typing.Generic[X]):
    """
    This class makes the binding between an entity class and the context
    in which its instances are created.
    """

    __entity_contexts: dict[tuple[X, Context], EntityContext[X]] = dict()

    def __init__(self, entity_type: type[X], context: Context) -> None:
        super().__init__()
        self.entity_type = entity_type
        self.context = context
        self.logger = logging.getLogger(f"{entity_type.__name__}Context")

        self._queries: dict[tuple[typing.Callable[[X], object], object], asyncio.Future[X]] = dict()
        self._instances: set[X] = set()

        self._frozen = False

    @property
    def frozen(self) -> bool:
        return self._frozen

    @property
    def queries(
        self,
    ) -> typing.Mapping[tuple[typing.Callable[[X], object], object], asyncio.Future[X]]:
        """
        Get the queries dict, as a frozen dict, it should not be tempered with.
        """
        return self._queries

    @property
    def instances(self) -> typing.FrozenSet[X]:
        """
        Get the instances set, as a frozen set, it should not be tempered with.
        """
        return self._instances  # type: ignore

    def add_instance(self, instance: X) -> None:
        """
        Add a new instance to the set of known instance.  Go through all the pending
        queries to see if this instance can resolve any of them.
        """
        if self.frozen:
            raise ContextModifiedAfterFreezeException(f"Can not add instance {instance} to context, it is already frozen.")

        # Register this instance
        self.logger.debug("Register instance %s", str(instance))
        self._instances.add(instance)

        # Go across all the queries to see if any can be resolved with this new
        # instance
        resolved: list[tuple[typing.Callable[[X], object], object]] = []
        for (index, arg), future in self._queries.items():
            if index(instance) == arg:
                self.logger.debug("Resolving query %s=%s with %s", str(index), str(arg), str(instance))
                future.set_value(instance)  # type: ignore
                resolved.append((index, arg))

        for res in resolved:
            self._queries.pop(res)

    def add_query(self, *, query: typing.Callable[[X], Y], result: Y) -> typing.Awaitable[X]:
        """
        Register a new query, every time a new instance is added, we will check if it matches
        the query.  If it does not check if any existing instance is already a match.  For this
        you need to use find_instance.
        """
        if self.frozen:
            raise ContextModifiedAfterFreezeException(f"Can not add query {query}={result} to context, it is already frozen.")

        identifier = (query, result)
        if identifier in self._queries:
            return self._queries[identifier]

        self.logger.debug("Register query %s=%s", str(query), str(result))
        self._queries[identifier] = asyncio.Future(loop=self.context.event_loop)
        return self._queries[identifier]

    def find_instance(self, *, query: typing.Callable[[X], Y], result: Y) -> X:
        """
        Find an return the first instance with matching index.  The query will
        be called on each known instance, and if the result matches the provided
        one, the instance is a match.
        If none can be found, raise a LookupError.
        """
        for instance in self._instances:
            if query(instance) == result:
                self.logger.debug(
                    "Instance %s is a match for %s=%s",
                    str(instance),
                    str(query.__name__),
                    str(result),
                )
                return instance

        raise LookupError(f"Could not find any instance matching the query {query}={result} " f"in {self._instances}")

    def freeze(self) -> None:
        """
        Freeze the current context, after this call, it will be impossible to add
        any instance, query or callback to this context.
        """
        self.logger.info("Freezing context")
        self._frozen = True

        if self._queries:
            self.logger.warning("Context has been frozen while some queries are still pending")

    def reset(self) -> None:
        self.logger.info("Resetting context")
        self._queries = dict()
        self._instances = set()
        self._frozen = False

    @classmethod
    def get(
        cls,
        *,
        entity_type: type[X],
        context: Context | None = None,
    ) -> EntityContext[X]:
        """
        Get the context instance for this entity_type and this base context.
        If none exists yet, and create_ok is True, then a new one is created.
        """
        if context is None:
            context = Context()

        identifier = (entity_type, context)
        if identifier not in cls.__entity_contexts:
            cls.__entity_contexts[identifier] = EntityContext(entity_type, context)

        return cls.__entity_contexts[identifier]
