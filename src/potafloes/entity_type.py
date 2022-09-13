from __future__ import annotations

import dataclasses
import inspect
import logging
import re
import sys
import typing

from potafloes import attachment, attribute, entity_context, exceptions

X = typing.TypeVar("X")
INDEX_MARKER = "entity_index"
DOUBLE_BIND_MARKER = "double_bind"
ENTITY_TYPES: set[EntityType] = set()
TYPE_ANNOTATION_EXPRESSION = re.compile(
    r"([a-zA-Z\.\_]+)(?:\[([a-zA-Z\.\_]+)(?:\,([a-zA-Z\.\_]+))*\])?"
)


def index(func: typing.Callable[[object], X]) -> typing.Callable[[object], X]:
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


@dataclasses.dataclass
class EntityTypeAnnotation:
    """
    This represents an entity typed field, it can be either an attribute or an annotation.
    """

    class_name: str
    module_name: str
    attribute: str
    annotation: str
    globals: dict
    locals: dict

    def base_type(self) -> type:
        """
        Try to resolve the base class of the annotation type expression.  If it succeeds, returns
        the type resolved.

        :raise ValueError: If the type annotation is not valid.
        :raise NameError: If the type annotation can not be resolved.
        """
        type_expression = TYPE_ANNOTATION_EXPRESSION.match(self.annotation)
        if not type_expression:
            raise ValueError(f"{repr(self.annotation)} is not a valid type annotation")

        # Try to evaluate base type, raise a NameError if the type can not be resolved
        return eval(type_expression.group(1), self.globals, self.locals)

    def __str__(self) -> str:
        return (
            ".".join([self.module_name, self.class_name, self.attribute])
            + f": {self.annotation}"
        )


class EntityType(type):
    def __init__(cls, name: str, __bases: tuple, __dict: dict, **kwds) -> None:
        super().__init__(name, __bases, __dict, **kwds)

        # Register the new entity type
        ENTITY_TYPES.add(cls)

        cls.name = name
        cls.logger = logging.getLogger(f"type({name})")

        cls._annotations: dict[str, EntityTypeAnnotation] | None = None
        cls._indices: dict[str, typing.Callable[[object], object]] | None = None
        cls._load_indices()

        cls._attachments: dict[str, attachment.AttachmentDefinition] = dict()
        cls._attributes: dict[str, attribute.AttributeDefinition] = dict()

        # Iterate over all the annotations of the class, and add them to the appropriate
        # dict, attributes or attachments
        for name, entity_annotation in cls._load_annotations().items():
            try:
                _type = entity_annotation.base_type()
            except NameError:
                # The type will be considered not to be an attachment
                cls._add_attribute(entity_annotation)
                continue

            if _type not in attachment.ATTACHMENT_TYPES:
                # The type is not an attachment, this is a simple attribute
                cls._add_attribute(entity_annotation)
                continue

            cls._add_attachment(entity_annotation, attachment_type=_type)

        cls._implementations: list[
            typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]
        ] = list()

    def __call__(cls, *args, **kwds: object) -> object:
        # We don't support non-positional arguments
        if args:
            raise ValueError("Entity creation doesn't support positional arguments")

        # This is the actual set of attributes that will be set on the object
        # We will go trough the argument provided in the call to the type and validate
        # each item, transforming them if required.
        kwargs: dict[str, object] = dict()

        to_be_bound: dict[str, attachment.Attachment] = dict()

        for arg, value in kwds.items():
            if arg in cls._attributes:
                # This is an attribute, we simply validate its type
                kwargs[arg] = cls._attributes[arg].validate(value)
                continue

            if arg in cls._attachments:
                # This is an attachment, if we have a value here, we need to replace it
                # and double-bind it with its replacement
                cls._attachments[arg].validate(value)
                assert isinstance(value, attachment.Attachment)
                to_be_bound[value._placeholder] = value
                continue

            raise ValueError(
                f"Unknown attribute passed to constructor: {cls.name} "
                f"doesn't have any attribute named {arg}"
            )

        # Build a new object, we take care later of checking whether it should be emitted or not
        new_object = super().__call__(**kwargs)

        # Get the entity context object for this type
        ec = entity_context.EntityContext[object].get(entity_type=cls)

        for index in cls._load_indices().values():
            try:
                instance = ec.find_instance(query=index, result=index(new_object))
                # This is a match, before returning the object, we should make sure that all our input
                # attributes are the same
                for key, value in kwargs.items():
                    instance_value = getattr(instance, key)
                    if instance_value == value:
                        continue

                    raise exceptions.DoubleSetException(
                        new_object, instance, key, index
                    )

                # If any attachment was provided in input, we need to double bind it with our
                # existing attachment in the current instance
                for a in to_be_bound.values():
                    current_attachment: attachment.Attachment = getattr(
                        instance, a._placeholder
                    )
                    a.subscribe(attachment=current_attachment)
                    current_attachment.subscribe(attachment=a)

                return instance
            except LookupError:
                continue

        # Once the object is created, with all the attributes, we also attach the attachment
        # objects.
        for placeholder, definition in cls._attachments.items():
            new_attachment = definition.attachment(new_object)
            object.__setattr__(new_object, placeholder, new_attachment)

            # If any attachment was provided in argument, we double-bind it with the new
            # attachment created for this instance
            if placeholder in to_be_bound:
                arg_attachment = to_be_bound[placeholder]
                arg_attachment.subscribe(attachment=new_attachment)
                new_attachment.subscribe(attachment=arg_attachment)

        # Trigger all the implementations for this newly created object
        for callback in cls._implementations:
            name = f"{callback}({new_object})"
            cls.logger.debug(
                "Trigger implementation %s (%s)",
                callback.__name__,
                inspect.getmodule(callback),
            )
            to_be_awaited = callback(new_object)
            ec.context.register(
                ec.context.event_loop.create_task(
                    to_be_awaited,
                    name=name,
                )
            )

        # Register the new instance in the context
        ec.add_instance(new_object)

        return new_object

    def __getattr__(cls, __name: str) -> object:
        """
        When getattr is called on the type instead of the instance, we return the corresponding
        attachment/attribute definition if one can be found.  Otherwise we default to the superclass
        method.
        """
        if __name in cls._attachments:
            return cls._attachments[__name]

        if __name in cls._attributes:
            return cls._attributes[__name]

        return super().__getattr__(__name)  # type: ignore

    def _bases(cls) -> typing.Generator[EntityType, None, None]:
        """
        Returns all the bases for this entity type which are also entity types.
        If anything else than an EntityType or an object is found in there, raise an error.

        :raise ValueError: When an invalid superclass is used in the entity definition.
        """
        for base in reversed(cls.__bases__):
            if base is object:
                continue

            if type(base) is not EntityType:
                raise ValueError(
                    f"Entity type {cls.name} extends {base.__name__}, this is forbidden."
                )

            yield base

    def _load_indices(cls) -> dict[str, typing.Callable[[object], object]]:
        """
        Get all the indices defined for this entity type.  Returns them as a generator,
        this also go through the base classes indices.
        Indices from the base classes are returned first.
        """
        if cls._indices is not None:
            return cls._indices

        cls._indices = dict()

        for base in cls._bases():
            cls.logger.debug(f"Subclass of {base.__name__}, reusing it's indices.")
            for entity_annotation in base._load_indices().values():
                cls._add_index(entity_annotation)

        for _, method in cls.__dict__.items():
            if hasattr(method, INDEX_MARKER):
                # This is an index
                cls._add_index(method)

        return cls._indices

    def _load_annotations(cls) -> dict[str, EntityTypeAnnotation]:
        """
        Get all the annotations for this entity type.
        """
        if cls._annotations is not None:
            return cls._annotations

        cls._annotations = dict()

        for base in cls._bases():
            cls.logger.debug(f"Subclass of {base.__name__}, reusing it's annotations.")
            for entity_annotation in base._load_annotations().values():
                cls._add_annotation(entity_annotation)

        cls.logger.debug(f"Reading annotations for {cls.name}")
        globals = getattr(sys.modules.get(cls.__module__, None), "__dict__", {})
        locals = dict(vars(cls))
        ann = cls.__dict__.get("__annotations__", {})

        for name, value in ann.items():
            cls.logger.debug(f"{cls.name}.{name}: {repr(value)} ({type(value)})")
            if not isinstance(value, str):
                raise ValueError(
                    f"Type {type(value)} is not a valid type annotation, expected str."
                )

            entity_annotation = EntityTypeAnnotation(
                class_name=cls.__name__,
                module_name=cls.__module__,
                attribute=name,
                annotation=value,
                globals=globals,
                locals=locals,
            )
            cls._add_annotation(entity_annotation)

        return cls._annotations

    def _add_index(cls, index: typing.Callable[[object], object]) -> None:
        """
        Add the provided index to the indices dict.  If an index with the same name is already
        present, log a warning and replace it.
        """
        if cls._indices is None:
            raise ValueError(f"Can not register {cls.name} index, indices dict is None")

        if index.__name__ in cls._indices:
            cls.logger.warning(
                f"{index.__name__} is defined in {cls._indices[index.__name__]} "
                f" and {index}, the later will overwrite the former."
            )

        cls._indices[index.__name__] = index

    def _add_annotation(cls, entity_annotation: EntityTypeAnnotation) -> None:
        """
        Add the provided annotation to the annotations dict.  If an annotation with the same
        name is already present, log a warning and replace it.
        """
        if cls._annotations is None:
            raise ValueError(
                f"Can not register {cls.name} annotation, annotations dict is None"
            )

        if entity_annotation.attribute in cls._annotations:
            cls.logger.warning(
                f"{entity_annotation.attribute} is defined in {cls._annotations[entity_annotation.attribute]} "
                f"and {entity_annotation}, the later will overwrite the former."
            )

        cls._annotations[entity_annotation.attribute] = entity_annotation

    def _add_attachment(
        cls,
        entity_annotation: EntityTypeAnnotation,
        *,
        attachment_type: type[attachment.Attachment],
    ) -> None:
        """
        Add the provided attachment to the attachments dict.  If an attachment with the same name
        is already present, raise an exception.

        :raise ValueError: When an attachment with the same name already exists.
        """
        if entity_annotation.attribute in cls._attachments:
            raise ValueError(
                f"There is already an attachment {entity_annotation.attribute} in {cls.name}.  "
                f"New {entity_annotation} conflicts with existing {cls._attachments[entity_annotation.attribute]}."
            )

        if hasattr(cls, entity_annotation.attribute):
            raise ValueError(
                f"Invalid value for {entity_annotation}, defaults are not allowed for attachments."
            )

        cls._attachments[entity_annotation.attribute] = attachment.AttachmentDefinition(
            bearer_class=cls,
            placeholder=entity_annotation.attribute,
            type_expression=entity_annotation.annotation,
            outer_type=attachment_type,
            globals=entity_annotation.globals,
            locals=entity_annotation.locals,
        )

    def _add_attribute(cls, entity_annotation: EntityTypeAnnotation) -> None:
        """
        Add the provided attribute to the attributes dict.  If an attribute with the same name
        is already present, raise an exception.

        :raise ValueError: When an attribute with the same name already exists.
        """
        if entity_annotation.attribute in cls._attributes:
            raise ValueError(
                f"There is already an attribute {entity_annotation.attribute} in {cls.name}.  "
                f"New {entity_annotation} conflicts with existing {cls._attributes[entity_annotation.attribute]}."
            )

        cls._attributes[entity_annotation.attribute] = attribute.AttributeDefinition(
            bearer_class=cls,
            placeholder=entity_annotation.attribute,
            type_expression=entity_annotation.annotation,
            globals=entity_annotation.globals,
            locals=entity_annotation.locals,
            default=getattr(cls, entity_annotation.attribute, None),
        )

    def _add_implementation(
        cls,
        implementation: typing.Callable[
            [X], typing.Coroutine[typing.Any, typing.Any, None]
        ],
    ) -> None:
        cls.logger.debug(f"Add implementation {implementation} to {cls}")
        cls._implementations.append(implementation)
