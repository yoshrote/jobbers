#!/usr/bin/env python3
r"""
Benchmark the Redis load a Jobbers *worker* generates as concurrency scales.

Drives the real worker code path (TaskGenerator + TaskProcessor, exactly what
runners/worker_proc.py runs) against a live Redis Stack instance, at a sweep of
WORKER_CONCURRENT_TASKS-equivalent concurrency levels, and reports:

  - end-to-end throughput (tasks/sec) as seen by the worker loop
  - Redis-side command throughput (ops/sec, from INFO commandstats deltas)
  - peak `connected_clients` observed on the Redis server during the run
  - used_memory delta attributable to the run
  - per-task latency percentiles (pop -> completion)

Requires a Redis Stack (RedisJSON + RediSearch) instance -- plain `redis:latest`
will not work, since this exercises the `redis_json` backend end to end
(TASK_BACKEND / DLQ_BACKEND / ROUTING_BACKEND all default to redis_json here).

Point this at a *throwaway* Redis instance. It creates/deletes bench-only
queues and roles and writes real task blobs; it does not touch unrelated keys,
but don't run it against a shared/production Redis.

Usage:
    docker run -d --name jobbers-bench-redis -p 16379:6379 redis/redis-stack-server:latest
    REDIS_URL=redis://localhost:16379 python scripts/redis_load_benchmark.py \\
        --concurrency 1,5,10,25,50,100,200 --tasks-per-level 2000

    # add artificial per-task work time (ms) to see throughput away from the
    # pure-Redis-bound ceiling (real tasks rarely take 0ms of their own work):
    REDIS_URL=redis://localhost:16379 python scripts/redis_load_benchmark.py \\
        --concurrency 5,25,100 --sleep-ms 50

    # raise the connection pool ceiling (redis-py's asyncio ConnectionPool defaults
    # max_connections to 100 per process, not unbounded):
    REDIS_URL=redis://localhost:16379 python scripts/redis_load_benchmark.py \\
        --concurrency 150 --submit-concurrency 150 --tasks-per-level 300 --max-connections 300

In production, jobbers itself is tuned the same way (no code changes needed) via a
query param on REDIS_URL, e.g. REDIS_URL="redis://host:6379?max_connections=300" --
--max-connections here is just this script's own knob for varying it across a sweep.

This benchmarks a single worker process's fetch loop (exactly what worker_proc.main()
runs: one sequential BZPOPMIN in flight, dispatching into a semaphore-gated pool of
executors). An earlier version of this script also prototyped parallel fetch loops
(multiple concurrent BZPOPMIN callers within one process) as an alternative way to push
past the single-loop ceiling; that prototype is gone. It surfaced a real correctness bug
(RedisRoutingNotifications caches one pub/sub connection per role, so concurrent
poll_refresh_signal() calls from independent fetchers race on the same connection reader)
and, once fixed, only bought modest throughput at a real latency cost. Running additional
worker *processes* scales linearly with none of that complexity or risk, so that's the
recommended lever -- see docs/benchmarking-and-performance.md.

Results are printed as a table and written as JSON to --out (default:
redis_load_benchmark_results.json in the current directory).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Force the repo's own `jobbers` package ahead of any stale installed copy in
# site-packages (running this as a script, rather than `python -m`, puts the
# script's own directory on sys.path[0] instead of the repo root).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Must be set before importing jobbers.db -- these are read at module-import time.
os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
os.environ.setdefault("TASK_BACKEND", "redis_json")
os.environ.setdefault("DLQ_BACKEND", "redis_json")
os.environ.setdefault("ROUTING_BACKEND", "redis_json")
os.environ.setdefault("TASK_SCHEDULER_BACKEND", "redis")
os.environ.setdefault("CRON_DAG_SCHEDULER_BACKEND", "redis")

import redis.asyncio as redis
from redis.exceptions import MaxConnectionsError
from ulid import ULID

from jobbers import db
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task import Task
from jobbers.models.task_config import DeadLetterPolicy
from jobbers.registry import clear_registry, register_task
from jobbers.task_generator import TaskGenerator
from jobbers.task_processor import TaskProcessor

if TYPE_CHECKING:
    from jobbers.state_manager import StateManager

BENCH_TASK_NAME = "bench_noop"
BENCH_TASK_VERSION = 1


@dataclass
class LevelResult:
    """Results of a single concurrency-level run in the sweep."""

    concurrency: int
    n_tasks: int
    sleep_ms: int
    max_connections: int
    submit_seconds: float
    submit_ops_per_sec: float
    run_seconds: float
    throughput_tasks_per_sec: float
    redis_ops_per_sec: float
    redis_connected_clients_baseline: int
    redis_connected_clients_peak: int
    redis_used_memory_delta_bytes: int
    latency_ms_p50: float
    latency_ms_p95: float
    latency_ms_p99: float
    latency_ms_max: float
    max_connections_error: bool = False
    tasks_completed: int = 0


@dataclass
class SweepResults:
    """All per-level results from a full concurrency sweep."""

    levels: list[LevelResult] = field(default_factory=list)


async def _register_bench_task(sleep_ms: int) -> None:
    clear_registry()

    async def _bench_noop(**_kwargs: object) -> dict[str, Any]:
        if sleep_ms:
            await asyncio.sleep(sleep_ms / 1000)
        return {}

    register_task(
        name=BENCH_TASK_NAME,
        version=BENCH_TASK_VERSION,
        max_retries=0,
        dead_letter_policy=DeadLetterPolicy.NONE,
    )(_bench_noop)


async def _ensure_queue_and_role(sm: StateManager, queue: str, role: str) -> None:
    await sm.routing.create_queue_config(QueueConfig(name=queue, max_concurrent=0))
    await sm.routing.create_role(role, {queue})


async def _submit_tasks(sm: StateManager, queue: str, n: int, submit_concurrency: int) -> float:
    sem = asyncio.Semaphore(submit_concurrency)

    async def _one() -> None:
        async with sem:
            task = Task(id=ULID(), name=BENCH_TASK_NAME, version=BENCH_TASK_VERSION, queue=queue)
            await sm.submit_task(task)

    start = time.perf_counter()
    await asyncio.gather(*(_one() for _ in range(n)))
    return time.perf_counter() - start


async def _redis_info(client: redis.Redis) -> dict[str, Any]:
    return await client.info()


async def _sample_peak_clients(client: redis.Redis, peak: list[int], stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            info = await client.info(section="clients")
            peak[0] = max(peak[0], int(info.get("connected_clients", 0)))
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            pass


async def _run_worker_loop(
    sm: StateManager, role: str, n_tasks: int, concurrency: int
) -> tuple[list[float], bool]:
    """
    Drain up to n_tasks tasks with up to `concurrency` in flight.

    Mirrors worker_proc.main() exactly: a single sequential fetch loop (one
    BZPOPMIN in flight at a time) dispatching into a semaphore-gated pool of
    executors.

    Returns (latencies_ms, hit_max_connections). If the pool is exhausted mid-run
    (redis.exceptions.MaxConnectionsError), stops early rather than crashing the
    whole sweep -- that failure point is itself the data point we want.
    """
    task_generator = TaskGenerator(sm, role, max_tasks=n_tasks)

    semaphore = asyncio.Semaphore(concurrency)
    latencies_ms: list[float] = []
    lock = asyncio.Lock()
    hit_max_connections = False
    active: list[asyncio.Task[None]] = []

    async def run_task(task: Task) -> None:
        nonlocal hit_max_connections
        popped_at = time.perf_counter()
        try:
            await TaskProcessor(sm).run(task)
        except MaxConnectionsError:
            hit_max_connections = True
        finally:
            elapsed_ms = (time.perf_counter() - popped_at) * 1000
            async with lock:
                latencies_ms.append(elapsed_ms)
            semaphore.release()

    try:
        while not hit_max_connections:
            await semaphore.acquire()
            if hit_max_connections:
                semaphore.release()
                break
            try:
                task = await anext(task_generator)
            except StopAsyncIteration:
                semaphore.release()
                break
            except MaxConnectionsError:
                semaphore.release()
                hit_max_connections = True
                break
            t = asyncio.create_task(run_task(task))
            active.append(t)
    finally:
        if active:
            await asyncio.gather(*active, return_exceptions=True)

    return latencies_ms, hit_max_connections


async def run_level(
    redis_client: redis.Redis,
    sm: StateManager,
    concurrency: int,
    n_tasks: int,
    sleep_ms: int,
    submit_concurrency: int,
    max_connections: int,
) -> LevelResult:
    queue = f"bench-{concurrency}-{sleep_ms}"
    role = f"bench-role-{concurrency}-{sleep_ms}"
    await _ensure_queue_and_role(sm, queue, role)

    submit_hit_max_connections = False
    try:
        submit_seconds = await _submit_tasks(sm, queue, n_tasks, submit_concurrency)
    except MaxConnectionsError:
        submit_hit_max_connections = True
        submit_seconds = 0.0

    try:
        baseline_info = await _redis_info(redis_client)
    except MaxConnectionsError:
        submit_hit_max_connections = True
        baseline_info = {}
    baseline_clients = int(baseline_info.get("connected_clients", 0))
    baseline_ops = int(baseline_info.get("total_commands_processed", 0))
    baseline_mem = int(baseline_info.get("used_memory", 0))

    peak_clients = [baseline_clients]
    stop_event = asyncio.Event()
    sampler = asyncio.create_task(_sample_peak_clients(redis_client, peak_clients, stop_event))

    start = time.perf_counter()
    latencies_ms: list[float] = []
    run_hit_max_connections = False
    if not submit_hit_max_connections:
        latencies_ms, run_hit_max_connections = await _run_worker_loop(sm, role, n_tasks, concurrency)
    run_seconds = time.perf_counter() - start

    stop_event.set()
    await sampler

    try:
        final_info = await _redis_info(redis_client)
    except MaxConnectionsError:
        run_hit_max_connections = True
        final_info = {}
    final_ops = int(final_info.get("total_commands_processed", baseline_ops))
    final_mem = int(final_info.get("used_memory", baseline_mem))

    latencies_ms.sort()

    def pct(p: float) -> float:
        if not latencies_ms:
            return 0.0
        idx = min(len(latencies_ms) - 1, int(len(latencies_ms) * p))
        return latencies_ms[idx]

    return LevelResult(
        concurrency=concurrency,
        n_tasks=n_tasks,
        sleep_ms=sleep_ms,
        max_connections=max_connections,
        submit_seconds=submit_seconds,
        submit_ops_per_sec=n_tasks / submit_seconds if submit_seconds else 0.0,
        run_seconds=run_seconds,
        throughput_tasks_per_sec=len(latencies_ms) / run_seconds if run_seconds else 0.0,
        redis_ops_per_sec=(final_ops - baseline_ops) / run_seconds if run_seconds else 0.0,
        redis_connected_clients_baseline=baseline_clients,
        redis_connected_clients_peak=peak_clients[0],
        redis_used_memory_delta_bytes=final_mem - baseline_mem,
        latency_ms_p50=pct(0.50),
        latency_ms_p95=pct(0.95),
        latency_ms_p99=pct(0.99),
        latency_ms_max=latencies_ms[-1] if latencies_ms else 0.0,
        max_connections_error=submit_hit_max_connections or run_hit_max_connections,
        tasks_completed=len(latencies_ms),
    )


def _print_table(results: list[LevelResult]) -> None:
    header = (
        f"{'conc':>5} {'sleep_ms':>8} {'done/n':>9} {'run_s':>7} {'tasks/s':>8} "
        f"{'redis_ops/s':>11} {'clients(base->peak)':>20} {'mem_delta_mb':>12} "
        f"{'p50_ms':>7} {'p95_ms':>7} {'p99_ms':>7} {'maxconn':>8}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        clients = f"{r.redis_connected_clients_baseline}->{r.redis_connected_clients_peak}"
        done = f"{r.tasks_completed}/{r.n_tasks}"
        print(
            f"{r.concurrency:>5} {r.sleep_ms:>8} {done:>9} {r.run_seconds:>7.2f} "
            f"{r.throughput_tasks_per_sec:>8.1f} {r.redis_ops_per_sec:>11.1f} {clients:>20} "
            f"{r.redis_used_memory_delta_bytes / 1e6:>12.2f} "
            f"{r.latency_ms_p50:>7.1f} {r.latency_ms_p95:>7.1f} {r.latency_ms_p99:>7.1f} "
            f"{'ERROR' if r.max_connections_error else '-':>8}"
        )


async def main_async(args: argparse.Namespace) -> None:
    await _register_bench_task(args.sleep_ms)

    # --max-connections is this script's own sweep parameter (distinct from how jobbers
    # itself is tuned in production, which is via a query param on REDIS_URL -- see
    # module docstring). 0 means "use redis-py's library default" (100 for the async
    # client as of 8.x, not unbounded, despite jobbers/db.py not setting it explicitly).
    redis_client = redis.from_url(
        os.environ["REDIS_URL"],
        protocol=3,
        legacy_responses=False,
        socket_timeout=None,
        max_connections=args.max_connections or None,
    )
    await db.set_client(redis_client)
    effective_max_connections = redis_client.connection_pool.max_connections

    sm = await db.init_state_manager()

    concurrency_levels = [int(c) for c in args.concurrency.split(",")]

    results: list[LevelResult] = []
    for concurrency in concurrency_levels:
        n_tasks = args.tasks_per_level or max(500, concurrency * 20)
        n_tasks = min(n_tasks, args.max_tasks_per_level)
        submit_concurrency = min(concurrency, args.submit_concurrency)
        print(f"\n=== concurrency={concurrency} n_tasks={n_tasks} sleep_ms={args.sleep_ms} ===")
        result = await run_level(
            redis_client,
            sm,
            concurrency,
            n_tasks,
            args.sleep_ms,
            submit_concurrency,
            effective_max_connections,
        )
        results.append(result)
        status = " *** MaxConnectionsError ***" if result.max_connections_error else ""
        print(
            f"  submit: {result.submit_seconds:.2f}s ({result.submit_ops_per_sec:.1f} tasks/s)  "
            f"run: {result.run_seconds:.2f}s ({result.tasks_completed}/{result.n_tasks} done, "
            f"{result.throughput_tasks_per_sec:.1f} tasks/s)  "
            f"redis: {result.redis_ops_per_sec:.1f} ops/s  "
            f"clients {result.redis_connected_clients_baseline}->{result.redis_connected_clients_peak}"
            f"{status}"
        )
        if result.max_connections_error:
            print(
                f"  Hit MaxConnectionsError at concurrency={concurrency} "
                f"(pool max_connections={effective_max_connections}); stopping sweep here."
            )
            break

    print("\n" + "=" * 40 + " SUMMARY " + "=" * 40)
    print(f"pool max_connections = {effective_max_connections}")
    _print_table(results)

    sweep = SweepResults(levels=results)
    await asyncio.to_thread(Path(args.out).write_text, json.dumps(asdict(sweep), indent=2))
    print(f"\nWrote results to {args.out}")

    await db.close_client()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--concurrency",
        default="1,5,10,25,50,100,200",
        help="Comma-separated list of WORKER_CONCURRENT_TASKS-equivalent concurrency levels to sweep.",
    )
    parser.add_argument(
        "--tasks-per-level",
        type=int,
        default=0,
        help="Tasks to process at each concurrency level. 0 = auto (max(500, concurrency*20)).",
    )
    parser.add_argument(
        "--max-tasks-per-level",
        type=int,
        default=5000,
        help="Cap on tasks-per-level, applied even when using auto-sizing (default 5000).",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Simulated per-task work time in ms (0 = pure Redis-bound ceiling; try 50-200 for a "
        "more realistic light-task scenario).",
    )
    parser.add_argument(
        "--submit-concurrency",
        type=int,
        default=20,
        help="Concurrency used to seed each level's queue (kept independent of the worker "
        "concurrency under test so submission itself doesn't exhaust the pool; default 20).",
    )
    parser.add_argument(
        "--max-connections",
        type=int,
        default=0,
        help="redis-py ConnectionPool max_connections for this benchmark run. 0 = library "
        "default (100 for the async client as of redis-py 8.x -- not unbounded). Kept as "
        "its own flag so it's easy to vary across a sweep, e.g. by scripting multiple "
        "invocations at different --max-connections values.",
    )
    parser.add_argument(
        "--out",
        default="redis_load_benchmark_results.json",
        help="Path to write JSON results.",
    )
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
