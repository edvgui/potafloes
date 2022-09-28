from __future__ import annotations

import asyncio
import typing

from potafloes import attachment, exceptions

X = typing.TypeVar("X", bound=object)


@attachment.entity_attachment
class Single(attachment.Attachment[X]):
    def __init__(
        self,
        bearer: object,
        placeholder: str,
        object_type: type[X],
        definition: attachment.AttachmentDefinition,
    ) -> None:
        super().__init__(bearer, placeholder, object_type, definition)
        self._completed: asyncio.Future[X] = asyncio.Future(loop=self._context.event_loop)
        self._context.register(self._completed)

    def _insert(self, item: X) -> None:
        if not self._completed.done():
            self._completed.set_result(item)
            return

        # The insert method can only be called once
        raise exceptions.DoubleSetException(self._bearer, self._placeholder, self._completed.result(), item)

    def _all(self) -> typing.Iterable[X]:
        if not self._completed.done():
            # If it is not done yet, the list is empty
            return []

        return [self._completed.result()]

    def __await__(self) -> typing.Generator[typing.Any, None, X]:
        return self._completed.__await__()
