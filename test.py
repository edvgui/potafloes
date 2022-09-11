from __future__ import annotations

import logging

from potafloes.attachments import bag
from potafloes.context import Context
from potafloes.entity import Entity, implementation, index


class Person(Entity):
    name: str
    likes_dogs: bool
    parents: bag.Bag[Person]
    children: bag.Bag[Person]

    @index
    def unique_name(self) -> str:
        return self.name

    async def praise_child(self, child: "Person") -> None:
        print(f"{self.name} says: {child.name} is my child and I am proud of him/her!")

    def __repr__(self) -> str:
        return f"Person(name={self.name})"


@implementation(Person)
async def handle_children(person: Person) -> None:
    print(f"Dealing with children of {person}")
    person.children.subscribe(callback=person.praise_child)
    person.children.subscribe(attachment=person.parents)


@implementation(Person)
async def handle_parents(person: Person) -> None:
    print(f"Dealing with parents of {person}")
    async def praise_parent(parent: Person) -> None:
        print(f"{person.name} says: {parent.name} is my parent")
    
    person.parents.subscribe(callback=praise_parent)


logging.basicConfig(level=logging.DEBUG)


async def main() -> None:

    will_be_bob = Person.get(index=Person.unique_name, arg="bob")

    bob = Person(name="bob", likes_dogs=True)
    marilyn = Person(name="marilyn", likes_dogs=False)

    marilyn.parents += bob

    bob_2 = await Person.get(index=Person.unique_name, arg="bob")
    print(bob == bob_2)
    print(bob == await will_be_bob)


Context().run(main)
Context().reset()
Context().run(main)
