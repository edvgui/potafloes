import asyncio
from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional, Sequence, Set, Tuple, Type, TypeVar
from ouat.bounded_stream import BoundedStream
from ouat.stream import Stream
from ouat.task_manager import TaskManager


X = TypeVar("X")
INDEX_MARKER = "entity_index"
IMPLEMENTATION_MARKER = "entity_implementation"
PRODUCT_MARKER = "product_marker"


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


def implementation(*, condition: Callable[["E"], Coroutine[Any, Any, bool]]) -> Callable[[Callable[["E"], Coroutine[Any, Any, None]]], None]:
    """
    Mark the current method as an implementation, which should only be selected if
    the condition resolves to True.
    """
    def implementation_input(func: Callable[["E"], Coroutine[Any, Any, bool]]) -> None:
        async def implementation_if_selected(self) -> None:
            selected = await condition(self)
            if not selected:
                return

            await func(self)

        setattr(implementation_if_selected, IMPLEMENTATION_MARKER, True)
        
        return implementation_if_selected
    
    return implementation_input


def stream(func: Callable[["E"], Type[X]]) -> Callable[["E"], Stream[X]]:
    """
    Mark the current method as a stream, on which will be added and potentially
    subscribed items
    """
    cache: Dict[Tuple["E", Callable[["E"], Type[X]]], Stream[X]] = dict()

    def stream_or_cache(self) -> Stream[X]:
        if (self, func) not in cache:
            print(f"Initiating stream for {func.__name__} of {self}")
            cache[(self, func)] = Stream(func(self))
        
        return cache[(self, func)]
    
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
                print(f"Initiating stream for {func.__name__} of {self}")
                cache[(self, func)] = BoundedStream(func(self), min=min, max=max)
            
            return cache[(self, func)]
        
        return stream_or_cache

    return bounded_stream_input


def output(__type: Type[X]) -> Awaitable[X]:
    return asyncio.Future()


class EntityType(type):
    _entities: Set[Type["E"]] = set()

    def __call__(cls: Type["E"], *args: Any, **kwds: Any) -> Any:
        if cls in cls._entities:
            return cls.__new__(cls, *args, **kwds)

        indices, implementations = [], []
        for _, method in cls.__dict__.items():
            if hasattr(method, INDEX_MARKER):
                # This is an index, we should register it
                indices.append(method)
                continue

            if hasattr(method, IMPLEMENTATION_MARKER):
                # This is an implementation, we should register it
                implementations.append(method)
                continue

        cls.register_indices(indices)
        cls.register_implementations(implementations)
        cls._entities.add(cls)
        
        return cls.__new__(cls, *args, **kwds)


class Entity(metaclass=EntityType):

    __indices: Sequence[Callable[["E"], X]]
    __implementations: Sequence[Callable[["E"], Coroutine[Any, Any, None]]]

    __queries: Optional[Dict[Tuple[Callable[["E"], X], X], asyncio.Future]] = None
    __instances: Optional[Set["E"]] = None

    def __new__(cls: type["E"], *args, **kwargs) -> "E":
        new_instance = object.__new__(cls)
        new_instance.__init__(*args, **kwargs)
        for instance in cls.instances():
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

        cls.instances().add(new_instance)

        loop = asyncio.get_running_loop()

        for implementation in cls.__implementations:
            to_be_awaited = implementation(new_instance)
            TaskManager.register(loop.create_task(to_be_awaited))

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
    def register_implementations(cls: Type["E"], implementations: Sequence[Callable[["E"], Coroutine[Any, Any, None]]]) -> None:
        cls.__implementations = implementations

    @classmethod
    def queries(cls: Type["E"]) -> Dict[Tuple[Callable[["E"], X], X], asyncio.Future]:
        if cls.__queries is None:
            cls.__queries = dict()
        return cls.__queries

    @classmethod
    def instances(cls: Type["E"]) -> Set["E"]:
        if cls.__instances is None:
            cls.__instances = set()
        return cls.__instances

    @classmethod
    async def get(cls: Type["E"], *, index=Callable[[Type["E"]], X], arg=X) -> "E":
        if index not in cls.__indices:
            raise ValueError(
                f"The provided index '{index.__name__}' can not be found on entity {cls.__name__}"
            )

        for instance in cls.instances():
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

E = TypeVar("E", bound=Entity)
