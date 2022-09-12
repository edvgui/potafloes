import asyncio
import typing

from potafloes import attachment, exceptions

X = typing.TypeVar("X")
Y = typing.TypeVar("Y")


class Single(attachment.Attachment[X]):
    def __init__(
        self,
        bearer: Y,
        placeholder: str,
        object_type: type[X],
        *,
        optional: bool,
    ) -> None:
        super().__init__(bearer, placeholder, object_type)
        self._optional = optional
        self._completed = asyncio.Future(loop=self._context.event_loop)

        self._context.register(self._completed)

    def _insert(self, item: X | None) -> bool:
        if item is None and self._optional:
            raise ValueError(f"Can not assign None to non-optional attachment: {self}")

        if self._completed.done():
            # The insert method can only be called once
            raise exceptions.DoubleSetException(
                self._bearer, self._bearer, self._placeholder, None
            )  # TODO

        self._completed.set_result(item)
        return True

    def _all(self) -> typing.Iterable[X]:
        if not self._completed.done():
            # If it is not done yet, the list is empty
            return []

        if self._completed.result() is None:
            # If it is done, but the result is None, the list is empty
            return []

        return [self._completed.result()]

    def __await__(self) -> typing.Generator[typing.Any, None, X]:
        return self._completed.__await__()
