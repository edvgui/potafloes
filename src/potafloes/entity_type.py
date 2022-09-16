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
TYPE_ANNOTATION_EXPRESSION = re.compile(r"([a-zA-Z\.\_]+)(?:\[([a-zA-Z\.\_]+)(?:\,([a-zA-Z\.\_]+))*\])?")
NO_DEFAULT = object()

Index = typing.Callable[[object], object]
Implementation = typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]]


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
        if hasattr(self, "_base_type_result"):
            return getattr(self, "_base_type_result")

        type_expression = TYPE_ANNOTATION_EXPRESSION.match(self.annotation)
        if not type_expression:
            raise ValueError(f"{repr(self.annotation)} is not a valid type annotation")

        # Try to evaluate base type, raise a NameError if the type can not be resolved
        setattr(
            self,
            "_base_type_result",
            eval(type_expression.group(1), self.globals, self.locals),
        )
        return getattr(self, "_base_type_result")

    def __str__(self) -> str:
        return ".".join([self.module_name, self.class_name, self.attribute]) + f": {self.annotation}"


class EntityType(type):
    def __init__(cls, name: str, __bases: tuple, __dict: dict, **kwds) -> None:
        super().__init__(name, __bases, __dict, **kwds)

        # Register the new entity type
        ENTITY_TYPES.add(cls)

        cls.name = name
        cls.logger = logging.getLogger(f"type({name})")

        cls.__indices: dict[str, Index] | None = None
        cls.__annotations: dict[str, EntityTypeAnnotation] | None = None
        cls.__attachments: dict[str, attachment.AttachmentDefinition] | None = None
        cls.__attributes: dict[str, attribute.AttributeDefinition] | None = None
        cls.__required_attributes: dict[str, attribute.AttributeDefinition] | None = None
        cls.__implementations: list[Implementation] | None = None

        cls.__registered_implementations: list[Implementation] = list()

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
            if arg in cls._attributes():
                # This is an attribute, we simply validate its type
                kwargs[arg] = cls._attributes()[arg].validate(value)
                continue

            if arg in cls._attachments():
                # This is an attachment, if we have a value here, we need to replace it
                # and double-bind it with its replacement
                cls._attachments()[arg].validate(value)
                assert isinstance(value, attachment.Attachment)
                to_be_bound[value._placeholder] = value
                continue

            raise ValueError(f"Unknown attribute passed to constructor: {cls.name} " f"doesn't have any attribute named {arg}")

        missing_attributes = cls._required_attributes().keys() - kwargs.keys()
        if missing_attributes:
            raise ValueError(f"The constructor is missing some parameters: {missing_attributes}")

        # Build a new object, we take care later of checking whether it should be emitted or not
        new_object = super().__call__(**kwargs)

        # Get the entity context object for this type
        ec = entity_context.EntityContext[object].get(entity_type=cls)

        for index in cls._indices().values():
            try:
                instance = ec.find_instance(query=index, result=index(new_object))
                # This is a match, before returning the object, we should make sure that all our input
                # attributes are the same
                for key, value in kwargs.items():
                    instance_value = getattr(instance, key)
                    if instance_value == value:
                        continue

                    raise exceptions.DoubleSetException(new_object, instance, key, index)

                # If any attachment was provided in input, we need to double bind it with our
                # existing attachment in the current instance
                for a in to_be_bound.values():
                    current_attachment: attachment.Attachment = getattr(instance, a._placeholder)
                    a.subscribe(attachment=current_attachment)
                    current_attachment.subscribe(attachment=a)

                return instance
            except LookupError:
                continue

        # Once the object is created, with all the attributes, we also attach the attachment
        # objects.
        for placeholder, definition in cls._attachments().items():
            new_attachment = definition.attachment(new_object)
            object.__setattr__(new_object, placeholder, new_attachment)

            # If any attachment was provided in argument, we double-bind it with the new
            # attachment created for this instance
            if placeholder in to_be_bound:
                arg_attachment = to_be_bound[placeholder]
                arg_attachment.subscribe(attachment=new_attachment)
                new_attachment.subscribe(attachment=arg_attachment)

        # Trigger all the implementations for this newly created object
        for callback in cls._implementations():
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
        if __name in cls._attachments():
            return cls._attachments()[__name]

        if __name in cls._attributes():
            return cls._attributes()[__name]

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
                raise ValueError(f"Entity type {cls.name} extends {base.__name__}, this is forbidden.")

            yield base

    def _indices(cls) -> dict[str, Index]:
        """
        Get all the indices defined for this entity type.  Returns them as a generator,
        this also go through the base classes indices.
        Indices from the base classes are returned first.
        """
        if cls.__indices is not None:
            return cls.__indices

        def add_index(indices: dict[str, Index], index: Index) -> None:
            """
            Add the provided index to the indices dict.  If an index with the same name is already
            present, log a warning and replace it.
            """
            if index.__name__ in indices:
                cls.logger.warning(
                    f"{index.__name__} is defined in {indices[index.__name__]} "
                    f" and {index}, the later will overwrite the former."
                )

            indices[index.__name__] = index

        cls.__indices = dict()

        for base in cls._bases():
            cls.logger.debug(f"Subclass of {base.__name__}, reusing it's indices.")
            for entity_annotation in base._indices().values():
                add_index(cls.__indices, entity_annotation)

        for _, method in cls.__dict__.items():
            if hasattr(method, INDEX_MARKER):
                # This is an index
                add_index(cls.__indices, method)

        return cls.__indices

    def _annotations(cls) -> dict[str, EntityTypeAnnotation]:
        """
        Get all the annotations for this entity type.  The computation is done lazily and
        cached.
        """
        if cls.__annotations is not None:
            return cls.__annotations

        cls.__annotations = dict()

        cls.logger.debug(f"Reading annotations for {cls.name}")
        globals = getattr(sys.modules.get(cls.__module__, None), "__dict__", {})
        locals = dict(vars(cls))
        ann = cls.__dict__.get("__annotations__", {})

        for name, value in ann.items():
            cls.logger.debug(f"{cls.name}.{name}: {repr(value)} ({type(value)})")
            if not isinstance(value, str):
                raise ValueError(f"Type {type(value)} is not a valid type annotation, expected str.")

            entity_annotation = EntityTypeAnnotation(
                class_name=cls.__name__,
                module_name=cls.__module__,
                attribute=name,
                annotation=value,
                globals=globals,
                locals=locals,
            )

            if entity_annotation.attribute in cls.__annotations:
                cls.logger.warning(
                    f"{entity_annotation.attribute} is defined in {cls.__annotations[entity_annotation.attribute]} "
                    f"and {entity_annotation}, the later will overwrite the former."
                )

            cls.__annotations[entity_annotation.attribute] = entity_annotation

        return cls.__annotations

    def _attachments(cls) -> dict[str, attachment.AttachmentDefinition]:
        """
        Return all the attachments for this type.  The computation of the attachments
        is done lazily and cached.
        The dict of attachments also contains the attachments from the parents.
        """
        if cls.__attachments is not None:
            return cls.__attachments

        def add_attachment(
            attachments: dict[str, attachment.AttachmentDefinition],
            a: attachment.AttachmentDefinition,
        ) -> None:
            if a.placeholder not in attachments:
                attachments[a.placeholder] = a
                return

            # We already have an attachment with that name, we only overwrite it
            # if we can.
            # The attachment can be overwritten if the type of the new attachment
            # is a subclass of the type of the old attachment
            existing_attachment = attachments[a.placeholder]
            if not issubclass(a.outer_type, existing_attachment.outer_type):
                raise ValueError(f"Can not overwrite {existing_attachment} with {a}: inconsistent attachment type.")

            try:
                if not issubclass(a.inner_type(), existing_attachment.inner_type()):
                    raise ValueError(f"Can not overwrite {existing_attachment} with {a}: inconsistent attachment type.")
            except NameError:
                # We get a name error when inner type can not be resolved yet
                # We will simply assume the inner type is set correctly
                cls.logger.warning(f"Can not verify type consistency between {existing_attachment} and {a}")

            cls.logger.debug(f"Overwriting attachment {existing_attachment} with {a}")
            attachments[a.placeholder] = a

        cls.__attachments = dict()

        for base in cls._bases():
            for a in base._attachments().values():
                add_attachment(cls.__attachments, a)

        for entity_annotation in cls._annotations().values():
            try:
                _type = entity_annotation.base_type()
            except NameError:
                # The type will be considered not to be an attachment
                continue

            if _type not in attachment.ATTACHMENT_TYPES:
                # The type is not an attachment, this is a simple attribute
                continue

            add_attachment(
                cls.__attachments,
                attachment.AttachmentDefinition(
                    bearer_class=cls,
                    placeholder=entity_annotation.attribute,
                    type_expression=entity_annotation.annotation,
                    outer_type=_type,
                    globals=entity_annotation.globals,
                    locals=entity_annotation.locals,
                ),
            )

        return cls.__attachments

    def _attributes(cls) -> dict[str, attribute.AttributeDefinition]:
        """
        Return all the attributes for this type.  The computation of the attributes
        is done lazily and cached.
        The dict of attributes also contains the attributes from the parents.
        """
        if cls.__attributes is not None:
            return cls.__attributes

        def add_attribute(
            attributes: dict[str, attribute.AttributeDefinition],
            a: attribute.AttributeDefinition,
        ) -> None:
            if a.placeholder not in attributes:
                attributes[a.placeholder] = a
                return

            # We already have an attribute with that name, we only overwrite it
            # if we can.
            # The attribute can be overwritten if the type of the new attribute
            # is a subclass of the type of the old attachment
            existing_attribute = attributes[a.placeholder]
            try:
                if not issubclass(a._type, existing_attribute._type):
                    raise ValueError(f"Can not overwrite {existing_attribute} with {a}: inconsistent attachment type.")
            except NameError:
                cls.logger.warning(f"Can not verify type consistency between {existing_attribute} and {a}")

            cls.logger.debug(f"Overwriting attribute {existing_attribute} with {a}")
            attributes[a.placeholder] = a

        cls.__attributes = dict()

        for base in cls._bases():
            for a in base._attributes().values():
                add_attribute(cls.__attributes, a)

        for entity_annotation in cls._annotations().values():
            try:
                _type = entity_annotation.base_type()
            except NameError:
                # The type will be considered not to be an attachment
                add_attribute(
                    cls.__attributes,
                    attribute.AttributeDefinition(
                        bearer_class=cls,
                        placeholder=entity_annotation.attribute,
                        type_expression=entity_annotation.annotation,
                        globals=entity_annotation.globals,
                        locals=entity_annotation.locals,
                        default=getattr(cls, entity_annotation.attribute, NO_DEFAULT),
                    ),
                )
                continue

            if _type not in attachment.ATTACHMENT_TYPES:
                # The type is not an attachment, this is a simple attribute
                add_attribute(
                    cls.__attributes,
                    attribute.AttributeDefinition(
                        bearer_class=cls,
                        placeholder=entity_annotation.attribute,
                        type_expression=entity_annotation.annotation,
                        globals=entity_annotation.globals,
                        locals=entity_annotation.locals,
                        default=getattr(cls, entity_annotation.attribute, NO_DEFAULT),
                    ),
                )
                continue

        return cls.__attributes

    def _required_attributes(cls) -> dict[str, attribute.AttributeDefinition]:
        if cls.__required_attributes is not None:
            return cls.__required_attributes

        cls.__required_attributes = {name: value for name, value in cls._attributes().items() if value.default is NO_DEFAULT}

        return cls.__required_attributes

    def _implementations(cls) -> list[Implementation]:
        """
        Aggregate all the implementations for this type into a list and cache it.
        This will take into account all the implementation defined on this specific type
        and all the once defined on any of its base classes.
        """
        if cls.__implementations is not None:
            return cls.__implementations

        cls.__implementations = list()

        for base in cls._bases():
            cls.__implementations.extend(base._implementations())

        cls.__implementations.extend(cls.__registered_implementations)

        return cls.__implementations

    def _add_implementation(
        cls,
        implementation: typing.Callable[[X], typing.Coroutine[typing.Any, typing.Any, None]],
    ) -> None:
        cls.logger.debug(f"Add implementation {implementation} to {cls}")
        cls.__registered_implementations.append(implementation)
