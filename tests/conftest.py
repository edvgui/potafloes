import importlib
import logging
import sys
import typing

import pytest

import potafloes
import potafloes.entity
import potafloes.entity_type

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def context() -> typing.Generator[potafloes.Context, None, None]:
    importlib.reload(potafloes.entity_type)
    importlib.reload(potafloes.entity)
    importlib.reload(potafloes)

    context = potafloes.Context.get()
    context.reset()

    yield context


@pytest.fixture(scope="function")
def attach_to_module(context: potafloes.Context) -> typing.Generator[typing.Callable[[type], None], None, None]:
    """
    This fixture is a way to allow defining classes in functions for the tests of this package.

    This is a patch for the issue reported here: https://peps.python.org/pep-0649/

        > Original Python semantics created a circular references problem for static typing analysis.
        > PEP 563 solved that problem–but its novel semantics introduced new problems, including its
        > restriction that annotations can only reference names at module-level scope.

    """
    modules_modifications: list[tuple[dict[str, type], type | None, type]] = []

    def attach_class_to_module(cls: type) -> None:
        LOGGER.debug(f"Making sure that {cls} is attached to its module")
        class_name = cls.__name__  # The name of the defined class
        class_module = sys.modules.get(
            cls.__module__, None
        )  # The module the class is defined in and should be attached to the dict of

        globals: dict[str, type] = getattr(class_module, "__dict__")
        previous_class: type | None = None
        if class_name in globals:
            previous_class = globals[class_name]
            LOGGER.warning(f"Replacing {previous_class} with {cls} in {globals}")

        globals[class_name] = cls
        modules_modifications.append((globals, previous_class, cls))

    yield attach_class_to_module

    for globals, previous_class, cls in modules_modifications:
        if previous_class is None:
            del globals[cls.__name__]
            continue

        globals[cls.__name__] = previous_class
