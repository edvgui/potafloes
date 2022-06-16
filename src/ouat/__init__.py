import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Coroutine, Dict, Generic, List, Optional, Sequence, Set, Tuple, Type, TypeVar


X = TypeVar("X")
INDEX_MARKER = "entity_index"
IMPLEMENTATION_MARKER = "entity_implementation"


def index(func: Callable[["O"], X]) -> Callable[["O"], X]:
    """
    Mark the current method as an index for the class it is a method of.
    """
    cache: Dict["O", X] = dict()

    def index_or_cache(self) -> X:
        if self not in cache:
            print(f"Computing index {func.__name__} for {self}")
            cache[self] = (func(self))

        return cache[self]

    setattr(index_or_cache, INDEX_MARKER, True)

    return index_or_cache


def implementation(*, condition: Callable[["O"], Coroutine[Any, Any, bool]]) -> Callable[[Callable[["O"], Coroutine[Any, Any, None]]], None]:
    """
    Mark the current method as an implementation, which should only be selected if
    the condition resolves to True.
    """
    def implementation_input(func: Callable[["O"], Coroutine[Any, Any, bool]]) -> None:
        async def implementation_if_selected(self) -> None:
            selected = await condition(self)
            if not selected:
                return

            await func(self)

        setattr(implementation_if_selected, IMPLEMENTATION_MARKER, True)
        
        return implementation_if_selected
    
    return implementation_input


def output(__type: Type[X]) -> Awaitable[X]:
    return asyncio.Future()


class EntityType(type):
    _entities: Set[Type["O"]] = set()

    def __call__(cls: Type["O"], *args: Any, **kwds: Any) -> Any:
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

    __indices: Sequence[Callable[["O"], X]]
    __implementations: Sequence[Callable[["O"], Coroutine[Any, Any, None]]]

    __queries: Optional[Dict[Tuple[Callable[["O"], X], X], asyncio.Future]] = None
    __instances: Optional[Set["O"]] = None

    def __new__(cls: type["O"], *args, **kwargs) -> "O":
        new_instance = object.__new__(cls)
        new_instance.__init__(*args, **kwargs)
        for instance in cls.instances():
            for indice in cls.__indices:
                if indice(new_instance) == indice(instance):
                    return instance
        
        resolved: List[Tuple[Callable[["O"], X], X]] = []
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
            loop.create_task(to_be_awaited)

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
    def register_indices(cls: Type["O"], indices: Sequence[Callable[["O"], X]]) -> None:
        cls.__indices = indices

    @classmethod
    def register_implementations(cls: Type["O"], implementations: Sequence[Callable[["O"], Coroutine[Any, Any, None]]]) -> None:
        cls.__implementations = implementations

    @classmethod
    def queries(cls: Type["O"]) -> Dict[Tuple[Callable[["O"], X], X], asyncio.Future]:
        if cls.__queries is None:
            cls.__queries = dict()
        return cls.__queries

    @classmethod
    def instances(cls: Type["O"]) -> Set["O"]:
        if cls.__instances is None:
            cls.__instances = set()
        return cls.__instances

    @classmethod
    async def get(cls: Type["O"], *, index=Callable[[Type["O"]], X], arg=X) -> "O":
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

O = TypeVar("O", bound=Entity)
