from typing import TYPE_CHECKING, Callable, TypeVar

if TYPE_CHECKING:
    from ouat.bounded_stream import BoundedStream
    from ouat.entity import E
    from ouat.stream import Stream


X = TypeVar("X")


class OuatException(RuntimeError):
    """
    Base class for all exceptions raised by this library.
    """


class ContextAlreadyInitializedException(BaseException):
    """
    Exception raised when an attempts is made to initialize an already
    initialized context
    """


class ContextAlreadyFrozenException(BaseException):
    """
    Exception raised when an attempts is made to freeze an already
    frozen context
    """


class ContextModifiedAfterFreezeException(BaseException):
    """
    Exception raised when an attempt is made to modify an entity context
    object after it has been frozen.
    """


class DomainModifiedAfterFreezeException(BaseException):
    """
    Exception raised when an attempt is made to modify an entity domain
    object after it has been frozen.
    """


class DoubleSetException(BaseException):
    """
    Exception raised when an entity is created twice with
    different attributes but matching index.
    """

    def __init__(
        self, a: "E", b: "E", attribute: str, index: Callable[["E"], X]
    ) -> None:
        self.entity_a = a
        self.value_a = getattr(a, attribute)

        self.entity_b = b
        self.value_b = getattr(b, attribute)

        self.attribute = attribute
        self.index = index

        super().__init__(
            f"Value set twice: {self}.  Matching index: "
            f"{type(a).__name__}.{index.__name__} = {index(a)}"
        )

    def __str__(self) -> str:
        return (
            f"{self.entity_a}.{self.attribute} != {self.entity_b}.{self.attribute} "
            f"({self.value_a} != {self.value_b})"
        )


class BoundedStreamOverflowException(BaseException):
    """
    Exception raised when a bounded stream receives too many items.
    """

    def __init__(self, bounded_stream: "BoundedStream[X]", item: X) -> None:
        self.bounded_stream = bounded_stream
        self.item = item

        super().__init__(f"Too many items in bounded stream: {self}")

    def __str__(self) -> str:
        return (
            f"{type(self.bounded_stream).__name__}@{self.bounded_stream._bearer}"
            f".{self.bounded_stream._placeholder} is already full, can not add "
            f"{self.item} to it.  {self.bounded_stream._count} items (or None) have "
            f"already been added: {self.bounded_stream._items}."
        )


class StreamItemTypeException(TypeError, BaseException):
    """
    Exception raised when an item of the wrong type is added to a stream.
    """

    def __init__(self, stream: "Stream[X]", item: object) -> None:
        self.stream = stream
        self.item = item

        super().__init__(f"Wrong item type in stream: {self}")

    def __str__(self) -> str:
        return (
            f"{type(self.stream).__name__}@{self.stream._bearer}.{self.stream._placeholder} "
            f"expects times of type {self.stream._object_type.__name__} but item {self.item} "
            f"has type {type(self.item).__name__}."
        )


class IncompleteBoundedStreamException(BaseException):
    """
    Exception raised when a bounded stream can not accept any more item
    but doesn't contain at least `min` elements.
    """

    def __init__(self, bounded_stream: "BoundedStream[X]") -> None:
        self.bounded_stream = bounded_stream

        super().__init__(f"Not enough items in bounded stream: {self}")

    def __str__(self) -> str:
        return (
            f"{type(self.bounded_stream).__name__}@{self.bounded_stream._bearer}"
            f".{self.bounded_stream._placeholder} is already complete, but doesn't "
            f"contain enough elements.  It expects at least {self.bounded_stream._min} "
            f"elements but only {len(self.bounded_stream._items)} where added."
        )
