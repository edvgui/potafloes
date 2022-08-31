from typing import Callable, Optional, Type
from mypy.plugin import Plugin, ClassDefContext
from mypy.nodes import TypeInfo
from mypy.plugins import dataclasses


ENTITY_FULLNAME = "ouat.entity.Entity"


class OuatPlugin(Plugin):
    def get_base_class_hook(self, fullname: str) -> Optional[Callable[[ClassDefContext], None]]:
        sym = self.lookup_fully_qualified(fullname)
        if sym and isinstance(sym.node, TypeInfo):  # pragma: no branch
            # No branching may occur if the mypy cache has not been cleared
            if any(base.fullname == ENTITY_FULLNAME for base in sym.node.mro):
                return lambda ctx: OuatEntityTransformer(ctx).transform()
        return None


class OuatEntityTransformer:
    def __init__(self, ctx: ClassDefContext) -> None:
        # We would have used inheritance here if it was possible.  But unfortunately, it
        # it not possible for an interpreted class to inherit a compiled class
        self.dataclass_transformer = dataclasses.DataclassTransformer(ctx)

    def transform(self) -> None:
        """Apply all the necessary transformations to the underlying
        dataclass so as to ensure it is fully type checked according
        to the rules in PEP 557.
        """
        ctx = self.dataclass_transformer._ctx
        info = self.dataclass_transformer._ctx.cls.info
        attributes = self.dataclass_transformer.collect_attributes()
        if attributes is None:
            # Some definitions are not ready. We need another pass.
            return False
        for attr in attributes:
            if attr.type is None:
                return False
        decorator_arguments = {
            "init": True,
            "eq": True,
            "order": False,
            "frozen": True,
            "slots": False,
            "match_args": True,
        }
        py_version = self.dataclass_transformer._ctx.api.options.python_version

        # If there are no attributes, it may be that the semantic analyzer has not
        # processed them yet. In order to work around this, we can simply skip generating
        # __init__ if there are no attributes, because if the user truly did not define any,
        # then the object default __init__ with an empty signature will be present anyway.
        if (
            decorator_arguments["init"]
            and ("__init__" not in info.names or info.names["__init__"].plugin_generated)
            and attributes
        ):

            args = [
                attr.to_argument()
                for attr in attributes
                if attr.is_in_init and not self.dataclass_transformer._is_kw_only_type(attr.type)
            ]

            if info.fallback_to_any:
                # Make positional args optional since we don't know their order.
                # This will at least allow us to typecheck them if they are called
                # as kwargs
                for arg in args:
                    if arg.kind == dataclasses.ARG_POS:
                        arg.kind = dataclasses.ARG_OPT

                nameless_var = dataclasses.Var("")
                args = [
                    dataclasses.Argument(nameless_var, dataclasses.AnyType(dataclasses.TypeOfAny.explicit), None, dataclasses.ARG_STAR),
                    *args,
                    dataclasses.Argument(nameless_var, dataclasses.AnyType(dataclasses.TypeOfAny.explicit), None, dataclasses.ARG_STAR2),
                ]

            dataclasses.add_method(ctx, "__init__", args=args, return_type=dataclasses.NoneType())

        if (
            decorator_arguments["eq"]
            and info.get("__eq__") is None
            or decorator_arguments["order"]
        ):
            # Type variable for self.dataclass_transformer types in generated methods.
            obj_type = ctx.api.named_type("builtins.object")
            self.dataclass_transformer_tvar_expr = dataclasses.TypeVarExpr(
                dataclasses.SELF_TVAR_NAME, info.fullname + "." + dataclasses.SELF_TVAR_NAME, [], obj_type
            )
            info.names[dataclasses.SELF_TVAR_NAME] = dataclasses.SymbolTableNode(dataclasses.MDEF, self.dataclass_transformer_tvar_expr)

        # Add <, >, <=, >=, but only if the class has an eq method.
        if decorator_arguments["order"]:
            if not decorator_arguments["eq"]:
                ctx.api.fail("eq must be True if order is True", ctx.cls)

            for method_name in ["__lt__", "__gt__", "__le__", "__ge__"]:
                # Like for __eq__ and __ne__, we want "other" to match
                # the self.dataclass_transformer type.
                obj_type = ctx.api.named_type("builtins.object")
                order_tvar_def = dataclasses.TypeVarType(
                    dataclasses.SELF_TVAR_NAME, info.fullname + "." + dataclasses.SELF_TVAR_NAME, -1, [], obj_type
                )
                order_return_type = ctx.api.named_type("builtins.bool")
                order_args = [
                    dataclasses.Argument(Var("other", order_tvar_def), order_tvar_def, None, dataclasses.ARG_POS)
                ]

                existing_method = info.get(method_name)
                if existing_method is not None and not existing_method.plugin_generated:
                    assert existing_method.node
                    ctx.api.fail(
                        f"You may not have a custom {method_name} method when order=True",
                        existing_method.node,
                    )

                dataclasses.add_method(
                    ctx,
                    method_name,
                    args=order_args,
                    return_type=order_return_type,
                    self_type=order_tvar_def,
                    tvar_def=order_tvar_def,
                )

        if decorator_arguments["frozen"]:
            self.dataclass_transformer._propertize_callables(attributes, settable=False)
            self.dataclass_transformer._freeze(attributes)
        else:
            self.dataclass_transformer._propertize_callables(attributes)

        if decorator_arguments["slots"]:
            self.dataclass_transformer.add_slots(info, attributes, correct_version=py_version >= (3, 10))

        self.dataclass_transformer.reset_init_only_vars(info, attributes)

        if (
            decorator_arguments["match_args"]
            and (
                "__match_args__" not in info.names or info.names["__match_args__"].plugin_generated
            )
            and attributes
            and py_version >= (3, 10)
        ):
            str_type = ctx.api.named_type("builtins.str")
            literals: dataclasses.List[Type] = [
                dataclasses.LiteralType(attr.name, str_type) for attr in attributes if attr.is_in_init
            ]
            match_args_type = dataclasses.TupleType(literals, ctx.api.named_type("builtins.tuple"))
            dataclasses.add_attribute_to_class(ctx.api, ctx.cls, "__match_args__", match_args_type)

        self.dataclass_transformer._add_dataclass_fields_magic_attribute()

        info.metadata["dataclass"] = {
            "attributes": [attr.serialize() for attr in attributes],
            "frozen": decorator_arguments["frozen"],
        }

        return True



def plugin(version: str) -> Type[Plugin]:
    return OuatPlugin
