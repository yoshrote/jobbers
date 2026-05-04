from __future__ import annotations

import contextlib
import inspect
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, get_args, get_origin, get_type_hints

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator

logger = logging.getLogger(__name__)


class Depends:
    """
    Marker for FastAPI-style dependency injection in task functions.

    Usage::

        async def get_db() -> AsyncGenerator[AsyncSession, None]:
            async with session_factory() as session:
                yield session


        @register_task(name="my_task", version=1)
        async def my_task(
            record_id: int,
            db: Annotated[AsyncSession, Depends(get_db)],
        ) -> dict: ...
    """

    __slots__ = ("dependency",)

    def __init__(self, dependency: Callable[..., Any]) -> None:
        if not callable(dependency):
            raise TypeError(f"Depends() argument must be callable, got {dependency!r}")
        self.dependency = dependency

    def __repr__(self) -> str:
        return f"Depends({self.dependency!r})"


@dataclass
class DependencyNode:
    """One pre-computed node in the dependency graph for a task function."""

    provider: Callable[..., Any]
    # kwarg_name → provider callable for each Depends() sub-dependency of this provider
    dep_params: dict[str, Callable[..., Any]] = field(default_factory=dict)
    is_async_generator: bool = False
    is_sync_generator: bool = False


# ---------------------------------------------------------------------------
# Graph inspection — called once at @register_task decoration time
# ---------------------------------------------------------------------------


def _extract_depends(hint: Any) -> Depends | None:
    """Return the Depends marker from an Annotated hint, or None."""
    import typing

    if get_origin(hint) is not typing.Annotated:
        return None
    for meta in get_args(hint)[1:]:
        if isinstance(meta, Depends):
            return meta
    return None


def _build_graph(
    func: Callable[..., Any],
    visited: dict[Callable[..., Any], DependencyNode],
    visiting: set[Callable[..., Any]],
) -> list[DependencyNode]:
    """DFS graph builder; returns topologically sorted nodes (leaves first)."""
    if func in visiting:
        raise ValueError(f"Dependency cycle detected involving {func!r}")

    if func in visited:
        return []

    visiting.add(func)
    ordered: list[DependencyNode] = []

    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}

    dep_params: dict[str, Callable[..., Any]] = {}
    for param_name, hint in hints.items():
        if param_name == "return":
            continue
        dep = _extract_depends(hint)
        if dep is None:
            continue
        dep_params[param_name] = dep.dependency
        # Recurse into the sub-dependency's own graph
        sub_nodes = _build_graph(dep.dependency, visited, visiting)
        ordered.extend(sub_nodes)

    visiting.discard(func)

    node = DependencyNode(
        provider=func,
        dep_params=dep_params,
        is_async_generator=inspect.isasyncgenfunction(func),
        is_sync_generator=inspect.isgeneratorfunction(func),
    )
    visited[func] = node
    ordered.append(node)
    return ordered


def inspect_task_dependencies(func: Callable[..., Any]) -> list[DependencyNode]:
    """
    Return the topologically-sorted dependency graph for *func*.

    Called once at @register_task decoration time.
    Raises ValueError on dependency cycles.
    Only nodes that are reachable via Depends() annotations are included.
    Nodes with no Depends() params are excluded (the task function itself is not a node).
    """
    visited: dict[Callable[..., Any], DependencyNode] = {}
    visiting: set[Callable[..., Any]] = set()

    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}

    ordered: list[DependencyNode] = []
    for param_name, hint in hints.items():
        if param_name == "return":
            continue
        dep = _extract_depends(hint)
        if dep is None:
            continue
        sub_nodes = _build_graph(dep.dependency, visited, visiting)
        ordered.extend(n for n in sub_nodes if n not in ordered)

    return ordered


def get_injected_param_names(func: Callable[..., Any]) -> frozenset[str]:
    """Return the set of param names on *func* that are Depends()-annotated."""
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        return frozenset()
    return frozenset(
        name for name, hint in hints.items() if name != "return" and _extract_depends(hint) is not None
    )


# ---------------------------------------------------------------------------
# Runtime override registry (test hook)
# ---------------------------------------------------------------------------

_dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] = {}


@contextlib.contextmanager
def dependency_overrides(
    overrides: dict[Callable[..., Any], Callable[..., Any]],
) -> Generator[None, None, None]:
    """
    Context manager for test-time dependency overrides.

    Usage::

        async def fake_db() -> AsyncSession:
            return FakeSession()


        with dependency_overrides({get_db: fake_db}):
            await process_payment.submit(...)
    """
    _dependency_overrides.update(overrides)
    try:
        yield
    finally:
        for key in overrides:
            _dependency_overrides.pop(key, None)


# ---------------------------------------------------------------------------
# Per-execution resolver
# ---------------------------------------------------------------------------


class DependencyResolver:
    """
    Resolves and executes the dependency graph for one task invocation.

    Use as an async context manager — cleanup of generator providers runs in
    __aexit__ regardless of whether the task succeeded or raised.

    Usage::

        resolver = DependencyResolver(task.task_config.dependency_graph)
        async with resolver:
            dep_cache = await resolver.resolve_all()
            # inject dep_cache values into kwargs, then call task function
    """

    def __init__(self, graph: list[DependencyNode]) -> None:
        self._graph = graph
        self._cache: dict[Callable[..., Any], Any] = {}
        # Open generators pushed in resolution order; closed in reverse in __aexit__.
        self._open_async_generators: list[AsyncGenerator[Any, None]] = []
        self._open_sync_generators: list[Generator[Any, None, None]] = []

    async def __aenter__(self) -> DependencyResolver:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Close all open generators in reverse order; log but never re-raise cleanup errors."""
        for agen in reversed(self._open_async_generators):
            try:
                await agen.aclose()
            except Exception:
                logger.exception("Error during dependency cleanup for generator %r", agen)
        for sgen in reversed(self._open_sync_generators):
            try:
                sgen.close()
            except Exception:
                logger.exception("Error during dependency cleanup for generator %r", sgen)

    async def resolve_all(self) -> dict[Callable[..., Any], Any]:
        """
        Resolve all nodes in the graph and return a provider → value cache.

        Walk the graph in topological order (leaves first); each node is resolved
        at most once — shared deps yield the same instance to all consumers.
        """
        for node in self._graph:
            effective = _dependency_overrides.get(node.provider, node.provider)
            if effective in self._cache or node.provider in self._cache:
                continue
            await self._resolve_node(node)
        return self._cache

    async def _resolve_node(self, node: DependencyNode) -> Any:
        effective = _dependency_overrides.get(node.provider, node.provider)
        if effective in self._cache:
            self._cache[node.provider] = self._cache[effective]
            return self._cache[node.provider]
        if node.provider in self._cache:
            return self._cache[node.provider]

        # Resolve sub-dep kwargs first
        sub_kwargs: dict[str, Any] = {}
        for kwarg_name, sub_provider in node.dep_params.items():
            effective_sub = _dependency_overrides.get(sub_provider, sub_provider)
            if effective_sub not in self._cache and sub_provider not in self._cache:
                sub_node = next((n for n in self._graph if n.provider == sub_provider), None)
                if sub_node is not None:
                    await self._resolve_node(sub_node)
            sub_kwargs[kwarg_name] = self._cache.get(effective_sub) or self._cache.get(sub_provider)

        value = await self._call_provider(effective, sub_kwargs)
        self._cache[node.provider] = value
        self._cache[effective] = value
        return value

    async def _call_provider(self, provider: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
        if inspect.isasyncgenfunction(provider):
            gen: AsyncGenerator[Any, None] = provider(**kwargs)
            self._open_async_generators.append(gen)
            return await gen.__anext__()
        elif inspect.isgeneratorfunction(provider):
            sgen: Generator[Any, None, None] = provider(**kwargs)
            self._open_sync_generators.append(sgen)
            return next(sgen)
        elif inspect.iscoroutinefunction(provider):
            return await provider(**kwargs)
        else:
            return provider(**kwargs)
