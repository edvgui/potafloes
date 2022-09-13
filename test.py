from __future__ import annotations

import logging
logging.basicConfig(level=logging.DEBUG)

from potafloes.attachments import bag
from potafloes.context import Context
from potafloes.entity import Entity, implementation, exchange
from potafloes.entity_type import index


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


class Test(Person):
    pass


@implementation(Test)
@implementation(Person)
async def handle_children(person: Person) -> None:
    print(f"Dealing with children of {person}")
    person.children.subscribe(callback=person.praise_child)


@implementation(Test)
@implementation(Person)
async def handle_parents(person: Person) -> None:
    print(f"Dealing with parents of {person}")
    async def praise_parent(parent: Person) -> None:
        print(f"{person.name} says: {parent.name} is my parent")

    person.parents.subscribe(callback=praise_parent)


Person.parents < exchange() > Person.children  # TODO support inheritance


async def main() -> None:

    will_be_bob = Person.get(index=Person.unique_name, arg="bob")

    bob = Person(name="bob", likes_dogs=True)
    marilyn = Person(name="marilyn", likes_dogs=False)

    marilyn.parents += bob

    alice = Test(name="alice", likes_dogs=False, parents=marilyn.parents)

    bob_2 = await Person.get(index=Person.unique_name, arg="bob")
    print(bob == bob_2)
    print(bob == await will_be_bob)


Context().run(main)
Context().reset()
Context().run(main)

print(Person.children)