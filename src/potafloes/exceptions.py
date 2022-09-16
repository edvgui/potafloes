from typing import TYPE_CHECKING, Callable, TypeVar

if TYPE_CHECKING:
    from potafloes.attachment import Attachment
    from potafloes.entity import E


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

    def __init__(self, a: "E", b: "E", attribute: str, index: Callable[["E"], X]) -> None:
        self.entity_a = a
        self.value_a = getattr(a, attribute)

        self.entity_b = b
        self.value_b = getattr(b, attribute)

        self.attribute = attribute
        self.index = index

        super().__init__(f"Value set twice: {self}.  Matching index: " f"{type(a).__name__}.{index.__name__} = {index(a)}")

    def __str__(self) -> str:
        return f"{self.entity_a}.{self.attribute} != {self.entity_b}.{self.attribute} " f"({self.value_a} != {self.value_b})"


class AttachmentItemTypeException(TypeError, BaseException):
    """
    Exception raised when an item of the wrong type is added to a stream.
    """

    def __init__(self, attachment: "Attachment[X]", item: object) -> None:
        self.attachment = attachment
        self.item = item

        super().__init__(f"Wrong item type in attachment: {self}")

    def __str__(self) -> str:
        return (
            f"{type(self.attachment).__name__}@{self.attachment._bearer}.{self.attachment._placeholder} "
            f"expects times of type {self.attachment._object_type.__name__} but item {self.item} "
            f"has type {type(self.item).__name__}."
        )
