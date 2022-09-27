import contextvars

ENTITY_SCOPE = contextvars.ContextVar[object | None]("entity_scope", default=None)
