from __future__ import annotations

import dataclasses
import typing

X = typing.TypeVar("X")

Callback = typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]


@dataclasses.dataclass
class Definition:
    """
    For internal use only.  Holds the information about the attachment that
    needs to be created when building the entity.
    """

    bearer_class: type
    placeholder: str
    type_expression: str
    globals: dict
    locals: dict

    @property
    def _type(self) -> type:
        if not hasattr(self, "_type_result"):
            setattr(
                self,
                "_type_result",
                eval(self.type_expression, self.globals, self.locals),
            )

        return getattr(self, "_type_result")

    def validate(self, attribute: object) -> object:
        attribute_type = self._type
        if not isinstance(attribute, attribute_type):
            raise TypeError(
                f"{repr(attribute)} has type {type(attribute)}, expected {attribute_type} for {self}"
            )

        return attribute

    def __str__(self) -> str:
        return f"{self.bearer_class}.{self.placeholder}: {self.type_expression}"
