from __future__ import annotations

import inspect
import logging
import re
import sys
import typing

from potafloes import attachment
from potafloes.context import Context
from potafloes.entity_context import EntityContext
from potafloes.entity_domain import EntityDomain
from potafloes.exceptions import DoubleSetException

X = typing.TypeVar("X")
INDEX_MARKER = "entity_index"
DOUBLE_BIND_MARKER = "double_bind"
TYPE_ANNOTATION_EXPRESSION = re.compile(
    r"([a-zA-Z\.\_]+)(?:\[([a-zA-Z\.\_]+)(?:\,([a-zA-Z\.\_]+))*\])?"
)


def index(func: typing.Callable[[E], X]) -> typing.Callable[[E], X]:
    """
    Mark the current method as an index for the class it is a method of.
    """
    cached_result_attr = f"__{func.__name__}_index"

    def index_or_cache(self) -> X:
        if not hasattr(self, cached_result_attr):
            object.__setattr__(self, cached_result_attr, func(self))

        return getattr(self, cached_result_attr)

    setattr(index_or_cache, INDEX_MARKER, True)
    index_or_cache.__name__ = func.__name__

    return index_or_cache


def implementation(entity_type: type[E]):
    """
    Register a function that should be called for each instance of the specified entity type.
    """
    entity_domain = EntityDomain.get(entity_type=entity_type)

    def register_implementation(
        func: typing.Callable[[E], typing.Coroutine[typing.Any, typing.Any, None]]
    ) -> typing.Callable[[E], typing.Coroutine[typing.Any, typing.Any, None]]:
        entity_domain.add_implementation(implementation=func)
        return func

    return register_implementation


class EntityType(type):
    _entities: set[EntityType] = set()

    def __init__(cls, name: str, __bases: tuple, __dict: dict, **kwds) -> None:
        super().__init__(name, __bases, __dict, **kwds)

        entity_domain = EntityDomain[object].get(entity_type=cls)

        for _, method in cls.__dict__.items():
            if hasattr(method, INDEX_MARKER):
                # This is an index, we should register it
                entity_domain.add_index(index=method)
                continue

        # Go through all attribute annotations and save the ones which are attachments
        for attribute, attribute_type, globals, locals in cls._attributes():
            base_type = TYPE_ANNOTATION_EXPRESSION.match(attribute_type)
            if not base_type:
                # TODO warn for unparsable type
                continue

            # Try to evaluate base type
            try:
                _type = eval(base_type.group(1), globals, locals)
            except NameError:
                # The type is not resolved yet, this is a simple attribute
                continue

            if _type not in attachment.ATTACHMENT_TYPES:
                # The type is not an attachment, this is a simple attribute
                continue

            entity_domain.add_attachment(
                attachment_reference=attachment.AttachmentReference(
                    bearer_class=cls,
                    placeholder=attribute,
                    inner_type_expression=base_type.group(2),
                    outer_type=_type,
                    globals=globals,
                    locals=locals,
                )
            )

        cls._entities.add(cls)

    def _attributes(cls) -> typing.Generator[tuple[str, str, dict, dict], None, None]:
        for base in reversed(cls.__mro__):
            # This logic is only valid for python 3.10+ and when using __future__.annotations
            globals = getattr(sys.modules.get(base.__module__, None), "__dict__", {})
            locals = dict(vars(base))
            ann = base.__dict__.get("__annotations__", {})

            for name, value in ann.items():
                if not isinstance(value, str):
                    raise ValueError(
                        f"Type {type(value)} is not a valid type annotation, expected str."
                    )

                yield name, value, globals, locals


class Entity(metaclass=EntityType):
    """
    This is the base class to any data part of the single state model.
    """

    _context: Context
    """
    This value is set automatically when the instance is constructed
    """

    _logger: logging.Logger
    """
    This value is set automatically when the instance is constructed
    """

    _created: bool = False
    """
    This value is set to True once all the initial attribute assignment is done.
    From that point, __setattr__ will raise an exception.
    """

    def __new__(cls: type[E], **kwargs) -> E:
        new_instance = object.__new__(cls)

        # Initialize the object
        # Unwrap all the attributes
        for key, value in kwargs.items():
            setattr(new_instance, key, value)

        entity_domain = EntityDomain.get(entity_type=cls)
        # For each attachment, we now add an actual object where the placeholder
        # currently sits
        for a in entity_domain.attachments:
            current_attachment = getattr(new_instance, a.placeholder, None)
            new_attachment = a.attachment(new_instance)
            if current_attachment is not None:
                # Validate that the attachment received in argument (and attached to this
                # object instance) is valid
                current_attachment = a.validate(current_attachment)

                # There is already an attachment in place there, we should create a new one
                # for our instance, and create a double binding between them.
                new_attachment.subscribe(attachment=current_attachment)
                current_attachment.subscribe(attachment=new_attachment)

            setattr(new_instance, a.placeholder, new_attachment)

        # Setup un-mutable values
        new_instance._context = Context()
        new_instance._logger = logging.getLogger(str(new_instance))
        new_instance._created = True

        entity_context = EntityContext.get(
            entity_type=cls, context=new_instance._context
        )

        attachments_names = [a.placeholder for a in entity_domain.attachments]

        # Check if the new instance matches any of the other existing onces
        # based on the indices.  If this is the case, we should return the
        # previously created object instead of a new one.
        for indice in entity_domain.indices:
            try:
                instance = entity_context.find_instance(
                    query=indice, result=indice(new_instance)
                )
                # This is a match, before returning the object, we should
                # make sure that all our input attributes are the same
                for key, value in kwargs.items():
                    if key in attachments_names:
                        # We don't have to check the attachment
                        # They take care of any double assignment them self if they
                        # are sensitive to it
                        continue

                    instance_value = getattr(instance, key)
                    if instance_value == value:
                        continue

                    # Double set exception
                    raise DoubleSetException(new_instance, instance, key, indice)

                # If any attachment was provided in input, we need to double
                # bind it with our existing attachment in the current instance
                for a in entity_domain.attachments:
                    if a.placeholder not in kwargs:
                        continue

                    input_attachment = kwargs[a.placeholder]

                    # We must verify the input attachment
                    input_attachment = a.validate(input_attachment)

                    current_attachment: attachment.Attachment = getattr(
                        instance, a.placeholder
                    )
                    current_attachment.subscribe(attachment=input_attachment)
                    input_attachment.subscribe(attachment=current_attachment)

                # The existing instance if a match to ours, from now one we will use
                # the existing object instead
                return instance
            except LookupError:
                continue

        # The new instance should now be registered as part of all the existing instances
        entity_context.add_instance(new_instance)

        # For all the implementations that have been registered, we create a task
        # calling them and passing the current object
        for callback in entity_domain.implementations:
            new_instance._trigger_implementation(callback)

        return new_instance

    def __init__(self, **kwargs) -> None:
        ...

    def __setattr__(self, __name: str, __value: typing.Any) -> None:
        # If we don't get that here, we will fail to set _created to True
        created = self._created

        previous_value = getattr(self, __name, None)
        super().__setattr__(__name, __value)
        new_value = getattr(self, __name)

        if created and previous_value is not new_value:
            raise RuntimeError("Can not modify created entity.")

    def _trigger_implementation(
        self: E,
        callback: typing.Callable[[E], typing.Coroutine[typing.Any, typing.Any, None]],
    ) -> None:
        name = f"{callback}({self})"
        self._logger.debug(
            "Trigger implementation %s (%s)",
            callback.__name__,
            inspect.getmodule(callback),
        )
        to_be_awaited = callback(self)
        self._context.register(
            self._context.event_loop.create_task(
                to_be_awaited,
                name=name,
            )
        )

    def __str__(self) -> str:
        queries = [
            f"{index.__name__}={repr(index(self))}"
            for index in EntityDomain.get(entity_type=type(self)).indices
        ]
        return f"{type(self).__name__}[{', '.join(queries)}]"

    @classmethod
    async def get(cls: type[E], *, index=typing.Callable[[object], X], arg=X) -> E:
        """
        Query an instance of the current type using one of its index function and
        the set of values it is expected to return.  Those queries can only be made
        in a context where event loop is running or has run.
        """
        entity_domain = EntityDomain.get(entity_type=cls)
        if index not in entity_domain.indices:
            raise ValueError(
                f"The provided index '{index.__name__}' can not be found on entity {cls.__name__}"
            )

        entity_context = EntityContext.get(entity_type=cls)

        try:
            return entity_context.find_instance(query=index, result=arg)
        except LookupError:
            return await entity_context.add_query(query=index, result=arg)


E = typing.TypeVar("E", bound=Entity)
E1 = typing.TypeVar("E1", bound=Entity)
