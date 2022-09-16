from __future__ import annotations

import pytest

import potafloes
import potafloes.exceptions


@pytest.fixture
def person_class(context: potafloes.Context) -> type[potafloes.Entity]:
    class Person(potafloes.Entity):
        name: str
        age: int

        @potafloes.index
        def unique_name(self) -> str:
            return self.name

    return Person


def test_entity_recreation(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        bob = Person(name="bob", age=50)
        alice = Person(name="alice", age=51)
        assert bob is not alice
        assert bob is Person(name="bob", age=50)
        assert bob is await Person.get(index=Person.unique_name, arg="bob")

    context.run(main)


def test_double_set(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        Person(name="bob", age=50)
        with pytest.raises(potafloes.exceptions.DoubleSetException):
            Person(name="bob", age=51)

    context.run(main)
