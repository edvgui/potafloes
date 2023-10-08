from __future__ import annotations

import typing

import pytest

import potafloes
import potafloes.exceptions


@pytest.fixture
def person_class(
    context: potafloes.Context, attach_to_module: typing.Callable[[type]]
) -> type[potafloes.Entity]:
    class Person(potafloes.Entity):
        name: str

        @potafloes.index
        def unique_name(self) -> str:
            return self.name

    return Person


def test_basic(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    all_persons: list[Person] = []

    @potafloes.implementation(Person)
    async def save_person(person: Person) -> None:
        all_persons.append(person)

    async def main() -> None:
        Person(name="bob")
        Person(name="alice")

    context.run(main)

    person_context = potafloes.EntityContext.get(
        entity_type=person_class, context=context
    )
    bob = person_context.find_instance(query=Person.unique_name, result="bob")
    alice = person_context.find_instance(query=Person.unique_name, result="alice")

    assert len(all_persons) == 2
    assert bob in all_persons
    assert alice in all_persons


def test_inheritance(
    context: potafloes.Context,
    person_class: type[potafloes.Entity],
) -> None:
    Person = person_class

    all_persons: list[Person] = []
    all_dads: list[Dad] = []

    class Dad(Person):
        pass

    @potafloes.implementation(Person)
    async def save_person(person: Person) -> None:
        all_persons.append(person)

    @potafloes.implementation(Dad)
    async def save_dad(dad: Dad) -> None:
        all_dads.append(dad)

    async def main() -> None:
        Person(name="bob")
        Dad(name="bob")

    context.run(main)

    person_context = potafloes.EntityContext.get(
        entity_type=person_class, context=context
    )
    bob = person_context.find_instance(query=Person.unique_name, result="bob")
    dad_context = potafloes.EntityContext.get(entity_type=Dad, context=context)
    dad = dad_context.find_instance(query=Person.unique_name, result="bob")

    assert len(all_persons) == 2
    assert bob in all_persons
    assert dad in all_persons

    assert len(all_dads) == 1
    assert dad in all_dads
