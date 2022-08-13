import asyncio
import logging
from typing import Awaitable, Callable, Dict, FrozenSet, Generic, List, Mapping, Optional, Set, Tuple, Type, TypeVar

from ouat.context import Context
from ouat.exceptions import ContextModifiedAfterFreezeException

X = TypeVar("X")
Y = TypeVar("Y")


class EntityContext(Generic[X]):
    """
    This class makes the binding between an entity class and the context
    in which its instances are created.
    """

    __entity_contexts: Dict[Tuple[Type[Y], Context], "EntityContext[Y]"] = dict()

    def __init__(self, entity_type: Type[X], context: Context) -> None:
        super().__init__()
        self.entity_type = entity_type
        self.context = context
        self.logger = logging.getLogger(f"{entity_type.__name__}Context")

        self._queries: Dict[Tuple[Callable[[X], Y], Y], asyncio.Future] = dict()
        self._instances: Set[X] = set()

        self._frozen = False

    @property
    def frozen(self) -> bool:
        return self._frozen

    @property
    def queries(self) -> Mapping[Tuple[Callable[[X], Y], Y], asyncio.Future]:
        """
        Get the queries dict, as a frozen dict, it should not be tempered with.
        """
        return self._queries

    @property
    def instances(self) -> FrozenSet[X]:
        """
        Get the instances set, as a frozen set, it should not be tempered with.
        """
        return self._instances

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
        resolved: List[Tuple[Callable[[X], Y], Y]] = []
        for (index, arg), future in self._queries.items():
            if index(instance) == arg:
                self.logger.debug("Resolving query %s=%s with %s", str(index), str(arg), str(instance))
                future.set_value(instance)
                resolved.append((index, arg))

        for res in resolved:
            self._queries.pop(res)

    def add_query(self, *, query=Callable[[Type[X]], Y], result=Y) -> Awaitable[X]:
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
        self._queries[identifier] = asyncio.Future(loop=self.context.event_loop())
        return self._queries[identifier]

    def find_instance(self, *, query=Callable[[Type[X]], Y], result=Y) -> X:
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
        cls: Type["EntityContext[X]"],
        *,
        entity_type: Type[X],
        context: Optional[Context] = None,
    ) -> "EntityContext[X]":
        """
        Get the context instance for this entity_type and this base context.
        If none exists yet, and create_ok is True, then a new one is created.
        """
        if context is None:
            context = Context()

        identifier = (entity_type, context)
        if identifier in cls.__entity_contexts:
            return cls.__entity_contexts[identifier]

        cls.__entity_contexts[identifier] = EntityContext(entity_type, context)
        return cls.__entity_contexts[identifier]
