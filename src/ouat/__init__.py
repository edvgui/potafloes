import asyncio
from typing import Any, Awaitable, Callable, Coroutine, Dict, Generator, List, Optional, Sequence, Set, Tuple, Type, TypeVar
from ouat.bounded_stream import BoundedStream
from ouat.stream import Stream
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


def stream(func: Callable[["E"], Type[X]]) -> Callable[["E"], Stream[X]]:
    """
    Mark the current method as a stream, on which will be added and potentially
    subscribed items
    """
    cache: Dict[Tuple["E", Callable[["E"], Type[X]]], Stream[X]] = dict()

    def stream_or_cache(self) -> Stream[X]:
        if (self, func) not in cache:
            # print(f"Initiating stream for {func.__name__} of {self}")
            cache[(self, func)] = Stream(func(self))
        
        s = cache[(self, func)]
        # print(f"Got stream object for {self}.{func.__name__}: {s}")
        return s
    
    return stream_or_cache


def bounded_stream(*, min: int = 0, max: int) -> Callable[[Callable[["E"], Type[X]]], Callable[["E"], BoundedStream[X]]]:

    def bounded_stream_input(func: Callable[["E"], Type[X]]) -> Callable[["E"], BoundedStream[X]]:
        """
        Mark the current method as a stream, on which will be added and potentially
        subscribed items
        """
        cache: Dict[Tuple["E", Callable[["E"], Type[X]]], BoundedStream[X]] = dict()

        def stream_or_cache(self) -> BoundedStream[X]:
            if (self, func) not in cache:
                # print(f"Initiating stream for {func.__name__} of {self}")
                cache[(self, func)] = BoundedStream(func(self), min=min, max=max)
            
            s = cache[(self, func)]
            # print(f"Got stream object for {self}.{func.__name__}: {s}")
            return s
        
        return stream_or_cache

    return bounded_stream_input


def double_bind(
    a: Callable[["E"], Stream["E1"]],
    b: Callable[["E1"], Stream["E"]],
) -> None:
    setattr(a, DOUBLE_BIND_MARKER, b)
    setattr(b, DOUBLE_BIND_MARKER, a)


class EntityType(type):
    _entities: Set[Type["E"]] = set()

    def __call__(cls: Type["E"], *args: Any, **kwds: Any) -> Any:
        if cls in cls._entities:
            return cls.__new__(cls, *args, **kwds)

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
        
        return cls.__new__(cls, *args, **kwds)


class Entity(metaclass=EntityType):

    __indices: Sequence[Callable[["E"], X]]
    __double_bindings: Sequence[Callable[["E"], Stream["E1"]]]

    __queries: Optional[Dict[Tuple[Callable[["E"], X], X], asyncio.Future]] = None
    __instances: Optional[Set["E"]] = None
    __implementations: Optional[List[Callable[["Entity"], Coroutine[Any, Any, None]]]] = None

    def __new__(cls: type["E"], *args, **kwargs) -> "E":
        new_instance = object.__new__(cls)
        new_instance.__init__(*args, **kwargs)
        for instance in cls.__instances or set():
            for indice in cls.__indices:
                if indice(new_instance) == indice(instance):
                    return instance
        
        resolved: List[Tuple[Callable[["E"], X], X]] = []
        for (index, arg), future in cls.queries():
            if index(new_instance) == arg:
                future.set_value(new_instance)
                resolved.append((index, arg))

        for res in resolved:
            cls.queries().pop(res)

        cls.__emit__(instance=new_instance)

        for double_binding in cls.__double_bindings:
            # For each double binding, each time
            # an item is added to our side of the relation,
            # we should add ourself to the other side
            other_side: Callable[["E1"], Stream["E"]] = getattr(double_binding, DOUBLE_BIND_MARKER)
            async def add_to_other_side(item: "E1") -> None:
                print(f"Add {new_instance} to {other_side.__name__} of {item}")
                other_side(item).send(new_instance)

            double_binding(new_instance).subscribe(add_to_other_side)

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
    def __emit__(cls: Type["E"], *, instance: "E") -> None:
        loop = asyncio.get_running_loop()

        for callback in cls.__implementations or []:
            to_be_awaited = callback(instance)
            TaskManager.register(loop.create_task(to_be_awaited))

        if cls.__instances is None:
            cls.__instances = set()
        cls.__instances.add(instance)

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
