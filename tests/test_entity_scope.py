from __future__ import annotations

import typing

import potafloes
import potafloes.exceptions


def test_basic(
    context: potafloes.Context,
    attach_to_module: typing.Callable[[type], None],
) -> None:
    class Person(potafloes.Entity):
        name: str
        friend_name: potafloes.Single[str]
        friend: potafloes.Single[Person]

        @potafloes.index
        def unique_name(self) -> str:
            return self.name

    attach_to_module(Person)

    @potafloes.implementation(Person)
    async def create_friend(person: Person) -> None:
        print(person, type(person))
        friend_name = await person.friend_name
        print("Friend name is", friend_name)
        person.friend += Person(name=friend_name)

    async def main() -> None:
        bob = Person(name="bob")
        bob.friend_name += "alice"

        will_be_alice = Person.get(index=Person.unique_name, arg="alice")
        alice = await will_be_alice
        print("Set alice to be friend with bob")
        alice.friend_name.send("bob")

    context.run(main)

    person_context = potafloes.EntityContext.get(entity_type=Person, context=context)
    bob = person_context.find_instance(query=Person.unique_name, result="bob")
    alice = person_context.find_instance(query=Person.unique_name, result="alice")

    assert bob in alice.producers._all()
    assert bob in alice.products._all()
    assert alice in bob.producers._all()
    assert alice in bob.products._all()
