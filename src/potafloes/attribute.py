from __future__ import annotations

import dataclasses
import typing

from potafloes import definition

X = typing.TypeVar("X")

Callback = typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]


@dataclasses.dataclass
class AttributeDefinition(definition.Definition):
    """
    For internal use only.  Holds the information about the attachment that
    needs to be created when building the entity.
    """

    default: object
