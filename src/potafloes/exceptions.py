"""
Exceptions defined in this module are all part of the stable api
"""
from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from potafloes import attachment


X = TypeVar("X")


class PotafloesException(RuntimeError):
    """
    Base class for all exceptions raised by this library.
    """


class ContextAlreadyInitializedException(PotafloesException):
    """
    Exception raised when an attempts is made to initialize an already
    initialized context
    """


class ContextAlreadyFrozenException(PotafloesException):
    """
    Exception raised when an attempts is made to freeze an already
    frozen context
    """


class ContextModifiedAfterFreezeException(PotafloesException):
    """
    Exception raised when an attempt is made to modify an entity context
    object after it has been frozen.
    """


class DomainModifiedAfterFreezeException(PotafloesException):
    """
    Exception raised when an attempt is made to modify an entity domain
    object after it has been frozen.
    """


class DoubleSetException(ValueError, PotafloesException):
    """
    Exception raised when an entity is created twice with
    different attributes but matching index.
    """

    def __init__(self, entity: object, attribute: str, value_a: object, value_b: object) -> None:
        self.entity = entity
        self.attribute = attribute
        self.value_a = value_a
        self.value_b = value_b

        super().__init__(f"Value set twice: {self}.")

    def __str__(self) -> str:
        return f"{self.entity}.{self.attribute}: {self.value_a} != {self.value_b}"


class AttachmentItemTypeException(TypeError, PotafloesException):
    """
    Exception raised when an item of the wrong type is added to a stream.
    """

    def __init__(self, attachment: attachment.Attachment[X], item: object) -> None:
        self.attachment = attachment
        self.item = item

        super().__init__(f"Wrong item type in attachment: {self}")

    def __str__(self) -> str:
        return (
            f"{type(self.attachment).__name__}@{self.attachment._bearer}.{self.attachment._placeholder} "
            f"expects times of type {self.attachment._object_type.__name__} but item {self.item} "
            f"has type {type(self.item).__name__}."
        )
