import typing

from potafloes import attachment

X = typing.TypeVar("X")
Y = typing.TypeVar("Y")


@attachment.attachment
class Bag(attachment.Attachment[X]):
    """
    A bag object can be used to register callbacks, that will be called for
    each item that is sent into the bag.  A bag has no upper bound, it
    is potentially infinite (only limited by memory of the system).  You should
    never await for this to finish, it doesn't even have a notion of completion.
    """

    def __init__(self, bearer: Y, placeholder: str, object_type: type[X]) -> None:
        """
        :param bearer: The object this bag is attached to.
        :param placeholder: The name of the object function this bag is a placeholder for.
        :param object_type: The type of objects to expect in the bag.  Objects introduced
            in the bag are expected to be a subclass of this type.
        """
        super().__init__(bearer, placeholder, object_type)

        self._items: set[X] = set()

    def _insert(self, item: X | None) -> bool:
        if item is None:
            # We skip None items
            return False

        if item in self._items:
            # We skip items which are already in the set
            return False

        self._items.add(item)
        return True

    def _all(self) -> typing.Iterable[X]:
        return self._items
