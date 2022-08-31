import asyncio
import logging
from typing import Type

from ouat import bounded_stream, stream
from ouat.context import Context
from ouat.entity import Entity, double_bind, implementation, index


class Dog(Entity):
    name: str
    owner: "Person"

    @index
    def unique_owner(self) -> "Person":
        return self.owner

    @index
    def unique_name(self) -> str:
        return self.name

    async def bark(self) -> None:
        print(f"Bark bark {self.owner.name}, bark bark")

    def __repr__(self) -> str:
        return f"Dog(name={self.name})"


implementation(Dog)(Dog.bark)


class Person(Entity):
    name: str
    likes_dogs: bool

    @bounded_stream.bounded_stream(max=1)
    def dog(self) -> Type[Dog]:
        return Dog

    @bounded_stream.bounded_stream(max=2)
    def parents(self) -> Type["Person"]:
        return Person

    @stream.stream
    def children(self) -> Type["Person"]:
        return Person

    @index
    def unique_name(self) -> str:
        return self.name

    async def praise_child(self, child: "Person") -> None:
        print(f"{self.name} says: {child.name} is my child and I am proud of him/her!")

    async def walk_dog(self) -> None:
        potential_dog = await self.dog()
        if not potential_dog:
            return

        actual_dog = potential_dog.pop()
        print(f"'This is such a wonderful day to walk my dog' says {self.name}")
        print(f"'An I really like my dog: {actual_dog.name}'")

    def __repr__(self) -> str:
        return f"Person(name={self.name})"


@implementation(Person)
async def handle_children(person: Person) -> None:
    print(f"Dealing with children of {person}")
    person.children().subscribe(person.praise_child)


@implementation(Person)
async def handle_parents(person: Person) -> None:
    parents = await person.parents()
    print(f"{person.name} says: {parents} are my parents!")


@implementation(Person)
async def create_dog(person: Person) -> None:
    if person.likes_dogs:
        person.dog().send(Dog(name=f"{person.name}'s dog", owner=person))
    else:
        person.dog().send(None)


implementation(Person)(Person.walk_dog)

double_bind(Person.children, Person.parents)


logging.basicConfig(level=logging.DEBUG)


async def main() -> None:

    will_be_bob = Person.get(index=Person.unique_name, arg="bob")

    bob = Person(name="bob", likes_dogs=True)
    marilyn = Person(name="marilyn", likes_dogs=False)

    marilyn_parents = marilyn.parents() 

    marilyn_parents += bob
    marilyn.parents().send(None)
    bob.parents().send(None)
    bob.parents().send(None)

    bob_2 = await Person.get(index=Person.unique_name, arg="bob")
    print(bob == bob_2)
    print(bob == await will_be_bob)


Context().run(main)
Context().reset()
Context().run(main)
