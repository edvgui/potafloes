from __future__ import annotations
import asyncio

import functools
import logging
import re
import typing

from potafloes import attachment, context, entity_context, entity_type, const

X = typing.TypeVar("X")
INDEX_MARKER = "entity_index"
DOUBLE_BIND_MARKER = "double_bind"
TYPE_ANNOTATION_EXPRESSION = re.compile(r"([a-zA-Z\.\_]+)(?:\[([a-zA-Z\.\_]+)(?:\,([a-zA-Z\.\_]+))*\])?")


def double_bind(a: attachment.AttachmentDefinition, b: attachment.AttachmentDefinition) -> None:
    def balance_factory(
        this_side: attachment.AttachmentDefinition,
        other_side: attachment.AttachmentDefinition,
    ) -> typing.Callable[[object], typing.Coroutine[typing.Any, typing.Any, None]]:
        async def balance(
            this_side: attachment.AttachmentDefinition,
            other_side: attachment.AttachmentDefinition,
            this_obj: object,
        ) -> None:
            async def add_to_other_side(
                this_obj: object,
                other_side: attachment.AttachmentDefinition,
                other_obj: object,
            ) -> None:
                attached = getattr(other_obj, other_side.placeholder)
                attached.send(this_obj)

            attached = getattr(this_obj, this_side.placeholder)
            callback = functools.partial(add_to_other_side, this_obj, other_side)
            setattr(callback, "__name__", add_to_other_side.__name__)
            attached.subscribe(callback=callback)

        balance_function = functools.partial(balance, this_side, other_side)
        setattr(balance_function, "__name__", balance.__name__)
        return balance_function

    entity_type.implementation(a.bearer_class)(balance_factory(a, b))
    entity_type.implementation(b.bearer_class)(balance_factory(b, a))


class AttachmentExchanger:
    def __init__(self) -> None:
        self.attachment_a: attachment.AttachmentDefinition | None = None
        self.attachment_b: attachment.AttachmentDefinition | None = None

    def __gt__(self, other: object) -> object:
        if not isinstance(other, attachment.AttachmentDefinition):
            return NotImplemented

        if self.attachment_a is None:
            self.attachment_a = other
            return self

        if self.attachment_b is not None:
            raise RuntimeError("You can not reuse an exchanger")

        self.attachment_b = other

        double_bind(self.attachment_a, self.attachment_b)

        return None


exchange = AttachmentExchanger
"""
This is a "syntactic sugar" to create a double bind.
Instead of doing

    ..code-block::

        double_bind(A.bs, B.as)

You can now do

    ..code-block::

        A.bs < exchange() > B.as

"""


class Entity(metaclass=entity_type.EntityType):
    """
    This is the base class to any data part of the single state model.
    """

    def __init__(self, **kwargs: object) -> None:
        self._created = False

        for key, value in kwargs.items():
            setattr(self, key, value)

        self._context = context.Context.get()
        self._logger = logging.getLogger(str(self))

        creator = const.ENTITY_SCOPE.get()
        self._created_by: object | None
        if creator is None:
            self._logger.debug("Created in main context")
            self._created_by = None
        else:
            self._logger.debug(f"Created by {creator}")
            self._created_by = creator

        self._created = True

    def __setattr__(self, __name: str, __value: typing.Any) -> None:
        # If we don't get that here, we will fail to set _created to True
        if __name == "_created":
            return super().__setattr__(__name, __value)

        previous_value = getattr(self, __name, None)
        super().__setattr__(__name, __value)
        new_value = getattr(self, __name)

        if self._created and previous_value is not new_value:
            raise RuntimeError("Can not modify created entity.")

    def __str__(self) -> str:
        queries = [f"{index.__name__}={repr(index(self))}" for index in type(self)._indices().values()]
        return f"{type(self).__name__}[{', '.join(queries)}]"

    @classmethod
    async def get(cls: type[E], *, index: typing.Callable[[E], X], arg: X) -> E:
        """
        Query an instance of the current type using one of its index function and
        the set of values it is expected to return.  Those queries can only be made
        in a context where event loop is running or has run.
        """
        if index not in cls._indices().values():
            raise ValueError(f"The provided index '{index.__name__}' can not be found on entity {cls.__name__}")

        ec = entity_context.EntityContext.get(entity_type=cls)

        try:
            return ec.find_instance(query=index, result=arg)
        except LookupError:
            return await ec.add_query(query=index, result=arg)


E = typing.TypeVar("E", bound=Entity)
E1 = typing.TypeVar("E1", bound=Entity)
