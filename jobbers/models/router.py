"""
Router function models.

A **router** is a registered pure function that picks which task handles a
payload at runtime. It is declared in a mermaid DAG as a rhombus node
(``R{"route_by_tier"}``) whose outgoing ``-->`` edges are the candidate task
nodes it may select from.

- ``RouteTo`` — the value a router returns: a *selector* over those candidates.
- ``RouterConfig`` — registry entry, mirroring ``TaskConfig``'s shape.

See ``docs/mermaid-dag-spec.md`` for the diagram syntax and
``docs/resource-management.md`` for the queue's capacity and rate controls.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any

from pydantic import BaseModel, PlainSerializer, WithJsonSchema

SerializableRouter = Annotated[
    Callable[..., Any],
    WithJsonSchema({"type": "string", "readOnly": True}),
    PlainSerializer(
        lambda f: f"{f.__module__}.{f.__qualname__}",
        return_type=str,
        when_used="json",
    ),
]


@dataclass(frozen=True)
class RouteTo:
    """
    Selector over a router's candidate nodes. Must match exactly one.

    Only the fields you set are used to filter; the rest are ignored. A bare
    ``str`` return from a router function is shorthand for ``RouteTo(name)``.

    ```python
    RouteTo("fulfil_order")  # by name alone
    RouteTo("fulfil_order", queue="priority")  # name + queue
    RouteTo("fulfil_order", version=2)  # name + version
    ```

    Filtering by name alone is an error when the router has several candidates
    with that name -- say which queue you mean.
    """

    task: str
    queue: str | None = None
    version: int | None = None


class RouterConfig(BaseModel):
    """Registry entry for a ``@register_router``-decorated function."""

    name: str
    version: int
    function: SerializableRouter
