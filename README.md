# Potafloes

Dataflow library to build single state data structures.


**Example**:
```python
from __future__ import annotations

import functools

import potafloes


class Person(potafloes.Entity):
    name: str
    friends: potafloes.Bag[Person]

    @potafloes.index
    def unique_name(self) -> str:
        return self.name


@potafloes.implementation(Person)
async def greet_friends(person: Person) -> None:
    """
    Greet our friends, we simply print a message for each friend we have.
    """

    async def greet_friend(friend: Person) -> None:
        print(f"'I am glad to be your fiend {friend.name}' says {person}")

    person.friends.subscribe(callback=greet_friend)


class Child(Person):
    parents: potafloes.Bag[Parent]
    ancestors: potafloes.Bag[Parent]


@potafloes.implementation(Child)
async def gather_ancestors(child: Child) -> None:
    """
    Gather the ancestors of a child.  For this, we take the parents of the child
    and the ancestors of each of the child's parents.
    """

    async def get_parent_ancestors(parent: Child) -> None:
        child.ancestors += parent.ancestors

    child.ancestors += child.parents

    child.parents.subscribe(callback=get_parent_ancestors)


@potafloes.implementation(Child)
async def greet_child_family(child: Child) -> None:
    """
    Greet the members of our family.
    """

    async def greet_family_member(relation: str, member: Parent) -> None:
        print(f"'{member.name} is my {relation}' says {child}")

    child.parents.subscribe(callback=functools.partial(greet_family_member, "parent"))
    child.ancestors.subscribe(callback=functools.partial(greet_family_member, "ancestor"))


class Parent(Child):
    children: potafloes.Bag[Child]
    descendants: potafloes.Bag[Child]


@potafloes.implementation(Parent)
async def greet_parent_family(child: Parent) -> None:
    """
    Greet the members of our family.
    """

    async def greet_family_member(relation: str, member: Parent) -> None:
        print(f"'{member.name} is my {relation}' says {child}")

    child.children.subscribe(callback=functools.partial(greet_family_member, "child"))
    child.descendants.subscribe(callback=functools.partial(greet_family_member, "descendant"))


# Create a link between friends
Person.friends < potafloes.exchange() > Person.friends

# Create a link between parent's children and child's parents
Child.parents < potafloes.exchange() > Parent.children

# Create a link between parent's descendants and child's ancestors
Child.ancestors < potafloes.exchange() > Parent.descendants


async def build_belgian_monarchs_family_tree() -> None:
    leopold_1 = Parent(name="Leopold I")
    louise = Parent(name="Louise of Orléans")
    louise.friends += leopold_1

    leopold_2 = Parent(name="Leopold II")
    leopold_2.parents += leopold_1
    leopold_2.parents += louise

    marie_henriette = Parent(name="Marie Henriette of Austria")
    marie_henriette.friends += leopold_2

    philippe = Parent(name="Philippe")
    marie = Parent(name="Marie of Hohenzollern-Sigmaringen")
    marie.friends += philippe

    philippe.parents += leopold_1
    philippe.parents += louise

    albert_1 = Child(name="Albert I")
    albert_1.parents += philippe
    albert_1.parents += marie


potafloes.Context.get().run(build_belgian_monarchs_family_tree)

```
The code snippet above prints this to stdout:
```text
'I am glad to be your fiend Leopold I' says Parent[unique_name='Louise of Orléans']
'Leopold I is my parent' says Parent[unique_name='Leopold II']
'Louise of Orléans is my parent' says Parent[unique_name='Leopold II']
'I am glad to be your fiend Leopold II' says Parent[unique_name='Marie Henriette of Austria']
'Leopold I is my parent' says Parent[unique_name='Philippe']
'Louise of Orléans is my parent' says Parent[unique_name='Philippe']
'I am glad to be your fiend Philippe' says Parent[unique_name='Marie of Hohenzollern-Sigmaringen']
'Philippe is my parent' says Child[unique_name='Albert I']
'Marie of Hohenzollern-Sigmaringen is my parent' says Child[unique_name='Albert I']
'I am glad to be your fiend Louise of Orléans' says Parent[unique_name='Leopold I']
'Leopold I is my ancestor' says Parent[unique_name='Leopold II']
'Louise of Orléans is my ancestor' says Parent[unique_name='Leopold II']
'Leopold II is my child' says Parent[unique_name='Leopold I']
'Leopold II is my child' says Parent[unique_name='Louise of Orléans']
'I am glad to be your fiend Marie Henriette of Austria' says Parent[unique_name='Leopold II']
'Leopold I is my ancestor' says Parent[unique_name='Philippe']
'Louise of Orléans is my ancestor' says Parent[unique_name='Philippe']
'Philippe is my child' says Parent[unique_name='Leopold I']
'Philippe is my child' says Parent[unique_name='Louise of Orléans']
'I am glad to be your fiend Marie of Hohenzollern-Sigmaringen' says Parent[unique_name='Philippe']
'Philippe is my ancestor' says Child[unique_name='Albert I']
'Marie of Hohenzollern-Sigmaringen is my ancestor' says Child[unique_name='Albert I']
'Albert I is my child' says Parent[unique_name='Philippe']
'Albert I is my child' says Parent[unique_name='Marie of Hohenzollern-Sigmaringen']
'Leopold II is my descendant' says Parent[unique_name='Leopold I']
'Leopold II is my descendant' says Parent[unique_name='Louise of Orléans']
'Philippe is my descendant' says Parent[unique_name='Leopold I']
'Philippe is my descendant' says Parent[unique_name='Louise of Orléans']
'Albert I is my descendant' says Parent[unique_name='Philippe']
'Albert I is my descendant' says Parent[unique_name='Marie of Hohenzollern-Sigmaringen']
'Leopold I is my ancestor' says Child[unique_name='Albert I']
'Louise of Orléans is my ancestor' says Child[unique_name='Albert I']
'Albert I is my descendant' says Parent[unique_name='Leopold I']
'Albert I is my descendant' says Parent[unique_name='Louise of Orléans']
```