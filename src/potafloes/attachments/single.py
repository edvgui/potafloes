from __future__ import annotations

import asyncio
import typing

from potafloes import attachment, exceptions

X = typing.TypeVar("X")


class Single(attachment.Attachment[X]):
    def __init__(
        self,
        bearer: object,
        placeholder: str,
        object_type: type[X],
        *,
        optional: bool,
    ) -> None:
        super().__init__(bearer, placeholder, object_type)
        self._optional = optional
        self._completed: asyncio.Future[X] = asyncio.Future(loop=self._context.event_loop)

        self._context.register(self._completed)  # type: ignore

    def _insert(self, item: X) -> bool:
        if item is None and self._optional:
            raise ValueError(f"Can not assign None to non-optional attachment: {self}")

        if self._completed.done():
            # The insert method can only be called once
            raise exceptions.DoubleSetException(self._bearer, self._placeholder, self._completed.result(), item)

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
