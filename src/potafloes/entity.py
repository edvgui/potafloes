from __future__ import annotations

import logging
import re
import typing

from potafloes import attachment, context, entity_context, entity_type

X = typing.TypeVar("X")
INDEX_MARKER = "entity_index"
DOUBLE_BIND_MARKER = "double_bind"
TYPE_ANNOTATION_EXPRESSION = re.compile(r"([a-zA-Z\.\_]+)(?:\[([a-zA-Z\.\_]+)(?:\,([a-zA-Z\.\_]+))*\])?")


def implementation(entity_type: type[E]):
    """
    Register a function that should be called for each instance of the specified entity type.
    """

    def register_implementation(
        func: typing.Callable[[E], typing.Coroutine[typing.Any, typing.Any, None]]
    ) -> typing.Callable[[E], typing.Coroutine[typing.Any, typing.Any, None]]:
        entity_type._add_implementation(implementation=func)
        return func

    return register_implementation


def double_bind(a: attachment.AttachmentDefinition, b: attachment.AttachmentDefinition) -> None:
    def balance_factory(
        this_side: attachment.AttachmentDefinition,
        other_side: attachment.AttachmentDefinition,
    ) -> typing.Callable[[E], typing.Coroutine[typing.Any, typing.Any, None]]:
        async def balance(this_obj: object) -> None:
            async def add_to_other_side(other_obj: object) -> None:
                attached = getattr(other_obj, other_side.placeholder)
                attached.send(this_obj)

            attached = getattr(this_obj, this_side.placeholder)
            attached.subscribe(callback=add_to_other_side)

        return balance

    implementation(a.bearer_class)(balance_factory(a, b))
    implementation(b.bearer_class)(balance_factory(b, a))


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

    _context: context.Context = object()  # type: ignore
    """
    This value is set automatically when the instance is constructed
    """

    _logger: logging.Logger = object()  # type: ignore
    """
    This value is set automatically when the instance is constructed
    """

    _created: bool = False
    """
    This value is set to True once all the initial attribute assignment is done.
    From that point, __setattr__ will raise an exception.
    """

    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

        self._context = context.Context()
        self._logger = logging.getLogger(str(self))
        self._created = True

    def __setattr__(self, __name: str, __value: typing.Any) -> None:
        # If we don't get that here, we will fail to set _created to True
        created = self._created

        previous_value = getattr(self, __name, None)
        super().__setattr__(__name, __value)
        new_value = getattr(self, __name)

        if created and previous_value is not new_value:
            raise RuntimeError("Can not modify created entity.")

    def __str__(self) -> str:
        queries = [f"{index.__name__}={repr(index(self))}" for index in type(self)._indices().values()]
        return f"{type(self).__name__}[{', '.join(queries)}]"

    @classmethod
    async def get(cls: type[E], *, index=typing.Callable[[object], X], arg=X) -> E:
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
