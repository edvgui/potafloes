from __future__ import annotations

import typing

import pytest

import potafloes
import potafloes.exceptions


@pytest.fixture
def person_class(context: potafloes.Context, attach_to_module: typing.Callable[[type]]) -> type[potafloes.Entity]:
    class Person(potafloes.Entity):
        name: str
        parents: potafloes.Bag[Person]
        children: potafloes.Bag[Person]
        best_friend: potafloes.Single[Person | None]

        @potafloes.index
        def unique_name(self) -> str:
            return self.name

    Person.parents < potafloes.exchange() > Person.children

    attach_to_module(Person)

    return Person


def test_basic(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        bob = Person(name="bob")
        alice = Person(name="alice")

        bob.best_friend += alice

    context.run(main)


def test_reassign_value(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        bob = Person(name="bob")
        alice = Person(name="alice")

        bob.best_friend += alice
        bob.best_friend += alice  # reassigning the same value should work

        with pytest.raises(potafloes.exceptions.DoubleSetException):
            bob.best_friend += bob  # reassigning another value should fail

    context.run(main)


def test_mistyped(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    async def main() -> None:
        bob = Person(name="bob")

        with pytest.raises(TypeError):
            bob.best_friend += object()  # object has the wrong type

    context.run(main)


def test_double_bind(context: potafloes.Context, person_class: type[potafloes.Entity]) -> None:
    Person = person_class

    async def main() -> None:
        bob = Person(name="bob")
        alice = Person(name="alice")
        eve = Person(name="eve")

        # Alice is a child of bob
        bob.children += alice

        # All parents of alice are parents of eve (they are siblings)
        # And reciprocally
        eve.parents += alice.parents
        alice.parents += eve.parents

    context.run(main)

    person_context = potafloes.EntityContext.get(entity_type=person_class, context=context)
    bob = person_context.find_instance(query=Person.unique_name, result="bob")
    alice = person_context.find_instance(query=Person.unique_name, result="alice")
    eve = person_context.find_instance(query=Person.unique_name, result="eve")

    assert bob in alice.parents._all()
    assert bob in eve.parents._all()
    assert alice in bob.children._all()
    assert eve in bob.children._all()
