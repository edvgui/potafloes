import asyncio
import functools
from typing import Any, Awaitable, Optional

from ouat import Entity, index, implementation, output

async def ok(person: "Person") -> bool:
    return True

async def has_dog(person: "Person") -> bool:
    return await person.dog is not None


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

    @implementation(condition=ok)
    async def bark(self) -> None:
        print(f"Bark bark {self.owner.name}, bark bark")

    def __repr__(self) -> str:
        return f"Dog(name={self.name})"


class Person(Entity):
    def __init__(self, name: str, likes_dogs: bool) -> None:
        super().__init__()
        self.name = name
        self.likes_dogs = likes_dogs
        self.dog = output(Optional[Dog])

    @index
    def unique_name(self) -> str:
        return self.name

    @implementation(condition=ok)
    async def print_name(self) -> None:
        print(self.name)

    @implementation(condition=ok)
    async def create_dog(self) -> None:
        if self.likes_dogs:
            self.dog.set_result(Dog(f"{self.name}'s dog", self))
        else:
            self.dog.set_result(None)

    @implementation(condition=has_dog)
    async def walk_dog(self) -> None:
        dog = await self.dog
        print(f"'This is such a wonderful day to walk my dog' says {self.name}")
        print(f"'An I really like my dog: {dog.name}'")

    def __repr__(self) -> str:
        return f"Person(name={self.name})"

async def main() -> None:
    will_be_bob = Person.get(index=Person.unique_name, arg="bob")
    will_be_bob_2 = Person.get(index=Person.unique_name, arg="bob")

    bob = Person("bob", likes_dogs=True)
    marilyn = Person("marilyn", likes_dogs=False)

    bob_2 = await Person.get(index=Person.unique_name, arg="bob")
    print(bob == bob_2)
    print(bob == await will_be_bob)
    print(bob == await will_be_bob_2)

asyncio.run(main())
