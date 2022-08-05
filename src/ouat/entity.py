import asyncio
import functools
import logging
from dataclasses import dataclass
from typing import (Any, Callable, Coroutine, Dict, List, Optional, Sequence,
                    Set, Tuple, Type, TypeVar)

from ouat.context import Context
from ouat.exceptions import DoubleSetException
from ouat.stream import Stream

X = TypeVar("X")
INDEX_MARKER = "entity_index"
DOUBLE_BIND_MARKER = "double_bind"


def index(func: Callable[["E"], X]) -> Callable[["E"], X]:
    """
    Mark the current method as an index for the class it is a method of.
    """
    cache: Dict["E", X] = dict()

    def index_or_cache(self) -> X:
        if self not in cache:
            print(f"Computing index {func.__name__} for {self}")
            cache[self] = func(self)

        return cache[self]

    setattr(index_or_cache, INDEX_MARKER, True)

    return index_or_cache


def double_bind(
    a: Callable[["E"], Stream["E1"]],
    b: Callable[["E1"], Stream["E"]],
) -> None:
    setattr(a, DOUBLE_BIND_MARKER, (a.__name__, b.__name__))
    setattr(b, DOUBLE_BIND_MARKER, (b.__name__, a.__name__))


class EntityType(type):
    _entities: Set[Type["E"]] = set()

    def __call__(cls: Type["E"], **kwds: Any) -> Any:
        if cls in cls._entities:
            return cls.__new__(cls, **kwds)

        # All entities are frozen dataclasses, the only attributes
        # who can be modified are the streams
        dataclass(cls, frozen=True, kw_only=True)

        indices, dbbs = [], []
        for _, method in cls.__dict__.items():
            if hasattr(method, INDEX_MARKER):
                # This is an index, we should register it
                indices.append(method)
                continue

            if hasattr(method, DOUBLE_BIND_MARKER):
                # This is a double bind stream
                dbbs.append(method)
                continue

        cls._register_indices(indices)
        cls._register_double_bindings(dbbs)
        cls._entities.add(cls)

        return cls.__new__(cls, **kwds)


class Entity(metaclass=EntityType):
    """
    This is the base class to any data part of the single state model.
    """

    __indices: Sequence[Callable[["E"], X]]
    """
    This class attribute holds all the indices that can be used to query instances
    of this class.
    """

    __double_bindings: Sequence[Callable[["E"], Stream["E1"]]]
    """
    This class attribute holds all the double bindings between streams of attributes
    of this class instances and another entity class.
    """

    __queries: Optional[Dict[Tuple[Callable[["E"], X], X], asyncio.Future]] = None
    """
    This class attribute holds all the pending queries on instances of this class.
    """

    __instances: Optional[Set["E"]] = None
    """
    This class attribute holds all the existing instances of this class.
    """

    __implementations: Optional[
        List[Callable[["Entity"], Coroutine[Any, Any, None]]]
    ] = None
    """
    This class attribute holds all the coroutines that should be called for each instance
    of this class.
    """

    _context: Context
    """
    This value is set automatically when the instance is constructed
    """

    _logger: logging.Logger
    """
    This value is set automatically when the instance is constructed
    """

    def __new__(cls: type["E"], **kwargs) -> "E":
        new_instance = object.__new__(cls)
        new_instance.__init__(**kwargs)

        if cls.__instances is None:
            cls.__instances = set()

        if cls.__implementations is None:
            cls.__implementations = list()

        if cls.__queries is None:
            cls.__queries = dict()

        # Check if the new instance matches any of the other existing onces
        # based on the indices.  If this is the case, we should return the
        # previously created object instead of a new one.
        for instance in cls.__instances:
            for indice in cls.__indices:
                if indice(new_instance) == indice(instance):
                    # This is a match, before returning the object, we should
                    # make sure that all our input attributes are the same
                    for key, value in kwargs.items():
                        instance_value = getattr(instance, key)
                        if instance_value == value:
                            continue

                        # Double set exception
                        raise DoubleSetException(new_instance, instance, key, indice)

                    return instance

        # The new instance should now be registered as part of all the existing instances
        cls.__instances.add(new_instance)

        # Check if the new instance matches any of the pending queries.
        # If this is the case, we complete the query by sending it the new instance
        # and remove the query from the pending queue.
        resolved: List[Tuple[Callable[["E"], X], X]] = []
        for (index, arg), future in cls.__queries.items():
            if index(new_instance) == arg:
                future.set_value(new_instance)
                resolved.append((index, arg))

        for res in resolved:
            cls.__queries.pop(res)

        # For all the implementations that have been registered, we create a task
        # calling them and passing the current object
        for callback in cls.__implementations:
            new_instance._emit_task(callback)

        for double_binding in cls.__double_bindings:
            # For each double binding, each time
            # an item is added to our side of the relation,
            # we should add ourself to the other side
            this_side_stream, other_side_stream = getattr(
                double_binding, DOUBLE_BIND_MARKER
            )

            async def add_to_other_side(item: "E1", *, other_side_stream: str) -> None:
                other_side = getattr(item, other_side_stream)
                other_side().send(new_instance)

            this_side = getattr(new_instance, this_side_stream)
            this_side().subscribe(
                functools.partial(
                    add_to_other_side, other_side_stream=other_side_stream
                )
            )

        return new_instance

    def __post_init__(self) -> None:
        """
        This is called by dataclass each time a new object is created
        """
        object.__setattr__(self, "_context", Context.get())
        object.__setattr__(self, "_logger", logging.getLogger(str(self)))

    def _emit_task(self, callback: Callable[[X], Coroutine[Any, Any, None]]) -> str:
        name = f"{callback}({self})"
        self._logger.debug("Starting new task: %s", name)
        to_be_awaited = callback(self)
        self._context.register(
            self._context.event_loop().create_task(
                to_be_awaited,
                name=name,
            )
        )

    @classmethod
    def _register_indices(
        cls: Type["E"], indices: Sequence[Callable[["E"], X]]
    ) -> None:
        cls.__indices = indices

    @classmethod
    def _register_double_bindings(
        cls: Type["E"], dbbs: Sequence[Callable[["E"], Stream["E1"]]]
    ) -> None:
        cls.__double_bindings = dbbs

    @classmethod
    async def get(cls: Type["E"], *, index=Callable[[Type["E"]], X], arg=X) -> "E":
        """
        Query an instance of the current type using one of its index function and
        the set of values it is expected to return.  Those queries can only be made
        in a context where event loop is running or has run.
        """
        if index not in cls.__indices:
            raise ValueError(
                f"The provided index '{index.__name__}' can not be found on entity {cls.__name__}"
            )

        for instance in cls.__instances or set():
            if index(instance) == arg:
                return instance

        # The instance we are trying to get doesn't exist yet, so we wait for its
        # creation
        query = (index, arg)

        if cls.__queries is None:
            cls.__queries = dict()

        if query not in cls.__queries:
            # The query has already been created, we just start waiting for it as
            # well
            cls.__queries[query] = asyncio.Future(loop=Context.get().event_loop())

        return await cls.__queries[query]

    @classmethod
    def for_each(
        cls: Type["E"],
        *,
        call: Callable[["E"], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Run for each instance of this class, the callback function provided in
        argument.
        """
        for instance in cls.__instances or set():
            instance._emit_task(call)

        if cls.__implementations is None:
            cls.__implementations = []
        cls.__implementations.append(call)


E = TypeVar("E", bound=Entity)
E1 = TypeVar("E1", bound=Entity)
