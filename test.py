from __future__ import annotations

import logging

from potafloes.entity_context import EntityContext
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


@implementation(Person)
async def handle_children(person: Person) -> None:
    print(f"Dealing with children of {person}")
    person.children.subscribe(callback=person.praise_child)


@implementation(Person)
async def handle_parents(person: Person) -> None:
    print(f"Dealing with parents of {person}")
    async def praise_parent(parent: Person) -> None:
        print(f"{person.name} says: {parent.name} is my parent")

    person.parents.subscribe(callback=praise_parent)


Person.parents < exchange() > Person.children


async def main() -> None:

    will_be_bob = Person.get(index=Person.unique_name, arg="bob")

    bob = Person(name="bob", likes_dogs=True)
    marilyn = Person(name="marilyn", likes_dogs=False)

    marilyn.parents += bob

    Test(name="alice", likes_dogs=False, parents=marilyn.parents)

    bob_2 = await Person.get(index=Person.unique_name, arg="bob")
    assert bob == bob_2
    assert bob == await will_be_bob

Context().run(main)

person_context = EntityContext.get(entity_type=Person)
test_context = EntityContext.get(entity_type=Test)

bob = person_context.find_instance(query=Person.unique_name, result="bob")
assert bob.name == "bob"

alice = test_context.find_instance(query=Person.unique_name, result="alice")
assert alice.name == "alice"

marilyn = person_context.find_instance(query=Person.unique_name, result="marilyn")
assert marilyn.name == "marilyn"

assert alice in bob.children._all()
assert marilyn in bob.children._all()
assert bob in alice.parents._all()
assert bob in marilyn.parents._all()

assert Test.parents is Person.parents

Context().reset()
Context().run(main)
