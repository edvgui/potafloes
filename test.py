import asyncio
import functools
from typing import Any, Awaitable, Optional, Type

from ouat import Entity, bounded_stream, double_bind, index, output, stream
from ouat.task_manager import TaskManager


class Dog(Entity):
    def __init__(self, name: str, owner: "Person") -> None:
        super().__init__()
        self.name = name
        self.owner = owner

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


Dog.for_each(call=Dog.bark)


class Person(Entity):
    def __init__(self, name: str, likes_dogs: bool) -> None:
        super().__init__()
        self.name = name
        self.likes_dogs = likes_dogs

    @bounded_stream(max=1)
    def dog(self) -> Type[Dog]:
        return Dog

    @bounded_stream(max=2)
    def parents(self) -> Type["Person"]:
        return Person

    @stream
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

async def handle_children(person: Person) -> None:
    person.children().subscribe(person.praise_child)

async def handle_parents(person: Person) -> None:
    parents = await person.parents()
    print(f"{person.name} says: {parents} are my parents!")

async def create_dog(person: Person) -> None:
    if person.likes_dogs:
        person.dog().send(Dog(f"{person.name}'s dog", person))
    else:
        person.dog().send(None)

Person.for_each(call=create_dog)
Person.for_each(call=Person.walk_dog)
Person.for_each(call=handle_children)
Person.for_each(call=handle_parents)

double_bind(
    Person.children,
    Person.parents,
)

async def main() -> Person:
    print("=== Start ===")
    will_be_bob = Person.get(index=Person.unique_name, arg="bob")
    will_be_bob_2 = Person.get(index=Person.unique_name, arg="bob")

    bob = Person("bob", likes_dogs=True)
    marilyn = Person("marilyn", likes_dogs=False)
    marilyn.parents().send(bob)
    marilyn.parents().send(None)
    bob.parents().send(None)
    bob.parents().send(None)

    bob_2 = await Person.get(index=Person.unique_name, arg="bob")
    print(bob == bob_2)
    print(bob == await will_be_bob)
    print(bob == await will_be_bob_2)

    await TaskManager.gather()
    print("=== Finish ===")

    return bob

bob = asyncio.run(main())
print(bob.children().items)
print(bob.children().callbacks)
