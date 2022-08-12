import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, Set, Tuple, Type, TypeVar

from ouat.context import Context
from ouat.entity_context import EntityContext
from ouat.entity_domain import EntityDomain
from ouat.exceptions import DoubleSetException
from ouat.stream import STREAM_MARKER, Stream

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
            cache[self] = func(self)

        return cache[self]

    setattr(index_or_cache, INDEX_MARKER, True)
    index_or_cache.__name__ = func.__name__

    return index_or_cache


def implementation(entity_type: Type["E"]):
    entity_domain = EntityDomain.get(entity_type=entity_type)

    def register_implementation(
        func: Callable[["E"], Coroutine[Any, Any, None]]
    ) -> Callable[["Entity"], Coroutine[Any, Any, None]]:
        entity_domain.add_implementation(implementation=func)
        return func

    return register_implementation


def double_bind(
    a: Callable[["E"], Stream["E1"]],
    b: Callable[["E1"], Stream["E"]],
) -> None:
    def balance_factory(
        this_side: Callable[["E"], Stream["E1"]],
        other_side: Callable[["E1"], Stream["E"]],
    ) -> Callable[[E], Coroutine[Any, Any, None]]:
        async def balance(this_obj: E) -> None:
            async def add_to_other_side(other_obj: E1) -> None:
                stream = other_side(other_obj)
                stream.send(this_obj)

            stream = this_side(this_obj)
            stream.subscribe(add_to_other_side)

        return balance

    entity_type_a = getattr(a, STREAM_MARKER)
    implementation(entity_type_a)(balance_factory(a, b))

    entity_type_b = getattr(b, STREAM_MARKER)
    implementation(entity_type_b)(balance_factory(b, a))


class EntityType(type):
    _entities: Set[Type["E"]] = set()

    def __init__(self, name: str, __bases: Tuple, __dict: Dict, **kwds) -> None:
        super().__init__(name, __bases, __dict, **kwds)

        # All entities are frozen dataclasses, the only attributes
        # who can be modified are the streams
        dataclass(self, frozen=True, kw_only=True)

        entity_domain = EntityDomain.get(entity_type=self)

        for _, method in self.__dict__.items():
            if hasattr(method, INDEX_MARKER):
                # This is an index, we should register it
                entity_domain.add_index(index=method)
                continue

            if hasattr(method, STREAM_MARKER):
                # This is a stream, we attach to the method the type of
                # the class it is on.
                setattr(method, STREAM_MARKER, self)
                continue

        self._entities.add(self)

    def __call__(self: Type["E"], **kwds: Any) -> Any:
        return super().__call__(
            **kwds, _context=Context(), _logger=logging.getLogger(__name__)
        )


class Entity(metaclass=EntityType):
    """
    This is the base class to any data part of the single state model.
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
        new_instance.__init__(
            **kwargs,
        )

        new_instance._logger.name = str(new_instance)

        entity_domain = EntityDomain.get(entity_type=cls)
        entity_context = EntityContext.get(
            entity_type=cls, context=new_instance._context
        )

        # Check if the new instance matches any of the other existing onces
        # based on the indices.  If this is the case, we should return the
        # previously created object instead of a new one.
        for indice in entity_domain.indices:
            try:
                instance = entity_context.find_instance(
                    query=indice, result=indice(new_instance)
                )
                # This is a match, before returning the object, we should
                # make sure that all our input attributes are the same
                for key, value in kwargs.items():
                    instance_value = getattr(instance, key)
                    if instance_value == value:
                        continue

                    # Double set exception
                    raise DoubleSetException(new_instance, instance, key, indice)

                return instance
            except LookupError:
                continue

        # The new instance should now be registered as part of all the existing instances
        entity_context.add_instance(new_instance)

        # For all the implementations that have been registered, we create a task
        # calling them and passing the current object
        for callback in entity_domain.implementations:
            new_instance._trigger_implementation(callback)

        return new_instance

    def _trigger_implementation(
        self, callback: Callable[[X], Coroutine[Any, Any, None]]
    ) -> str:
        name = f"{callback}({self})"
        self._logger.debug(
            "Trigger implementation %s (%s)",
            callback.__name__,
            inspect.getmodule(callback),
        )
        to_be_awaited = callback(self)
        self._context.register(
            self._context.event_loop.create_task(
                to_be_awaited,
                name=name,
            )
        )

    def __str__(self) -> str:
        queries = [
            f"{index.__name__}={index(self)}"
            for index in EntityDomain.get(entity_type=type(self)).indices
        ]
        return f"{type(self).__name__}[{', '.join(queries)}]"

    @classmethod
    async def get(cls: Type["E"], *, index=Callable[["E"], X], arg=X) -> "E":
        """
        Query an instance of the current type using one of its index function and
        the set of values it is expected to return.  Those queries can only be made
        in a context where event loop is running or has run.
        """
        entity_domain = EntityDomain.get(entity_type=cls)
        if index not in entity_domain.indices:
            raise ValueError(
                f"The provided index '{index.__name__}' can not be found on entity {cls.__name__}"
            )

        entity_context = EntityContext.get(entity_type=cls)

        try:
            return entity_context.find_instance(query=index, result=arg)
        except LookupError:
            return await entity_context.add_query(query=index, result=arg)


E = TypeVar("E", bound=Entity)
E1 = TypeVar("E1", bound=Entity)
