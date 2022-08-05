from typing import TYPE_CHECKING, Callable, TypeVar

if TYPE_CHECKING:
    from ouat.entity import E


X = TypeVar("X")


class OuatException(RuntimeError):
    """
    Base class for all exceptions raised by this library.
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
