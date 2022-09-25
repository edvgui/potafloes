from __future__ import annotations

import pytest

import potafloes


@pytest.fixture
def person_class(context: potafloes.Context) -> type[potafloes.Entity]:
    class Person(potafloes.Entity):
        name: str
        age: int
        likes_dogs: bool = True

    return Person


def test_basic(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        bob = Person(name="bob", age=50)
        assert bob.name == "bob"
        assert bob.age == 50
        assert bob.likes_dogs is True

        alice = Person(name="alice", age=51, likes_dogs=False)
        assert alice.name == "alice"
        assert alice.age == 51
        assert alice.likes_dogs is False

    context.run(main)


def test_reassign_value(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        bob = Person(name="bob", age=50)

        bob.name = "bob"  # reassigning the same value should work

        with pytest.raises(Exception):
            bob.name = "alice"  # reassigning another value should fail

    context.run(main)


def test_missing(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        with pytest.raises(ValueError):
            Person(name="bob")  # The constructor is missing an attribute

    context.run(main)


def test_excessive(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        with pytest.raises(ValueError):
            Person(name="bob", age=50, test="a")  # Test is not a valid attribute

    context.run(main)


def test_mistyped(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        with pytest.raises(TypeError):
            Person(name="bob", age="a")  # Age has the wrong type

    context.run(main)
