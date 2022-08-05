import asyncio
from dataclasses import KW_ONLY, dataclass
import functools
from typing import Any, Awaitable, Callable, Coroutine, Dict, Generator, List, Optional, Sequence, Set, Tuple, Type, TypeVar
from ouat.bounded_stream import BoundedStream, bounded_stream
from ouat.stream import Stream, stream
from ouat.task_manager import TaskManager


X = TypeVar("X")
INDEX_MARKER = "entity_index"
DOUBLE_BIND_MARKER = "double_bind"


async def true(_: "Entity") -> bool:
    return True


def index(func: Callable[["E"], X]) -> Callable[["E"], X]:
    """
    Mark the current method as an index for the class it is a method of.
    """
    cache: Dict["E", X] = dict()

    def index_or_cache(self) -> X:
        if self not in cache:
            print(f"Computing index {func.__name__} for {self}")
            cache[self] = (func(self))

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

        cls.register_indices(indices)
        cls.register_double_bindings(dbbs)
        cls._entities.add(cls)

        return cls.__new__(cls, **kwds)


class Entity(metaclass=EntityType):

    __indices: Sequence[Callable[["E"], X]]
    __double_bindings: Sequence[Callable[["E"], Stream["E1"]]]

    __queries: Optional[Dict[Tuple[Callable[["E"], X], X], asyncio.Future]] = None
    __instances: Optional[Set["E"]] = None
    __implementations: Optional[List[Callable[["Entity"], Coroutine[Any, Any, None]]]] = None

    def __new__(cls: type["E"], **kwargs) -> "E":
        new_instance = object.__new__(cls)
        new_instance.__init__(**kwargs)
        for instance in cls.__instances or set():
            for indice in cls.__indices:
                if indice(new_instance) == indice(instance):
                    for key, value in kwargs.items():
                        # Double set exception
                        assert getattr(instance, key) == value, f"{instance}.{key} != {value}"
                    return instance

        resolved: List[Tuple[Callable[["E"], X], X]] = []
        for (index, arg), future in cls.queries():
            if index(new_instance) == arg:
                future.set_value(new_instance)
                resolved.append((index, arg))

        for res in resolved:
            cls.queries().pop(res)

        loop = asyncio.get_running_loop()

        for callback in cls.__implementations or []:
            to_be_awaited = callback(new_instance)
            TaskManager.register(loop.create_task(to_be_awaited))

        if cls.__instances is None:
            cls.__instances = set()
        cls.__instances.add(new_instance)

        for double_binding in cls.__double_bindings:
            # For each double binding, each time
            # an item is added to our side of the relation,
            # we should add ourself to the other side
            this_side_stream, other_side_stream = getattr(double_binding, DOUBLE_BIND_MARKER)
            async def add_to_other_side(item: "E1", *, other_side_stream: str) -> None:
                other_side = getattr(item, other_side_stream)
                print(f"Received {item} in {new_instance}.{this_side_stream}, adding {new_instance} to {item}.{other_side_stream}")
                other_side().send(new_instance)

            this_side = getattr(new_instance, this_side_stream)
            this_side().subscribe(functools.partial(add_to_other_side, other_side_stream=other_side_stream))
            print(
                f"When and object is added to {new_instance}.{this_side_stream}, "
                f"self will be added to the object.{other_side_stream}"
            )

        return new_instance

    def __setattr__(self, __name: str, __value: Any) -> None:
        try:
            attribute = self.__getattribute__(__name)
            raise RuntimeError(
                f"Can not set value {__value} for existing attribute {__name}, "
                f"it already has value: {attribute}"
            )
        except AttributeError:
            pass

        super().__setattr__(__name, __value)

    @classmethod
    def register_indices(cls: Type["E"], indices: Sequence[Callable[["E"], X]]) -> None:
        cls.__indices = indices

    @classmethod
    def register_double_bindings(cls: Type["E"], dbbs: Sequence[Callable[["E"], Stream["E1"]]]) -> None:
        cls.__double_bindings = dbbs

    @classmethod
    def queries(cls: Type["E"]) -> Dict[Tuple[Callable[["E"], X], X], asyncio.Future]:
        if cls.__queries is None:
            cls.__queries = dict()
        return cls.__queries

    @classmethod
    async def get(cls: Type["E"], *, index=Callable[[Type["E"]], X], arg=X) -> "E":
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

        if query not in cls.queries():
            # The query has already been created, we just start waiting for it as
            # well
            cls.queries()[query] = asyncio.Future()

        return await cls.queries()[query]

    @classmethod
    def for_each(
        cls: Type["E"],
        *,
        call: Callable[["E"], Coroutine[Any, Any, None]],
    ) -> None:
        for instance in cls.__instances or set():
            loop = asyncio.get_running_loop()
            to_be_awaited = call(instance)
            TaskManager.register(loop.create_task(to_be_awaited))

        if cls.__implementations is None:
            cls.__implementations = []
        cls.__implementations.append(call)

E = TypeVar("E", bound=Entity)
E1 = TypeVar("E1", bound=Entity)
