import typing
import pytest
import importlib

import potafloes


@pytest.fixture(scope="function")
def context() -> typing.Generator[potafloes.Context, None, None]:
    importlib.reload(potafloes)

    context = potafloes.Context()
    yield context
