#!/usr/bin/env python3
"""
Benchmark the Postgres load a Jobbers worker generates as concurrency scales, when
TASK_BACKEND=sql (paired with DLQ_BACKEND=sql, TASK_SCHEDULER_BACKEND=sql,
ROUTING_BACKEND=sql).

Companion to scripts/redis_load_benchmark.py -- same structure and intent (drives the
real StateManager/TaskGenerator/TaskProcessor code path against a live backend), adapted
for SQL's very different pop semantics and connection-pool model. See
docs/benchmarking-and-performance.md for how to interpret results and compare against
the redis_json numbers there.

Even with every *_BACKEND set to "sql", Jobbers still requires a running Redis instance:
StateManager's cancellation bus and routing-refresh notifications (RedisCancellationBus /
RedisRoutingNotifications) are unconditionally Redis-backed regardless of TASK_BACKEND.
Point REDIS_URL at any plain Redis (redis:latest is fine -- Redis Stack/RedisJSON is not
required for this path).

SQLite is deliberately not supported here: its pool class (StaticPool) rejects
pool_size/max_overflow tuning outright, and it serializes writers at the file level, so
it can't usefully represent the multi-connection concurrency this benchmark measures.
Point SQL_PATH at Postgres, e.g. postgresql+asyncpg://user:pass@host:5432/db (requires
the `asyncpg` driver: `pip install -e ".[postgres]"` or `pip install asyncpg`).

Usage:
    docker compose up -d postgres redis
    SQL_PATH="postgresql+asyncpg://jobbers:jobbers@localhost:5432/jobbers" \\
    REDIS_URL=redis://localhost:6379 python scripts/sql_load_benchmark.py \\
        --concurrency 1,5,10,25,50,100 --tasks-per-level 1500

    # probe the connection-pool ceiling (SQLAlchemy's async engine defaults to
    # pool_size=5 + max_overflow=10 = 15 -- far tighter than redis-py's 100-connection
    # default). jobbers/db.py supports the same ?pool_size=...&max_overflow=...&
    # pool_timeout=... convention on SQL_PATH that REDIS_URL supports for redis-py --
    # SQLAlchemy doesn't do this natively, db.py pops the params off the URL itself
    # before constructing the engine; see docs/benchmarking-and-performance.md.
    SQL_PATH="...?pool_size=5&max_overflow=10" REDIS_URL=... python scripts/sql_load_benchmark.py \\
        --concurrency 50

Results are printed as a table and written as JSON to --out (default:
sql_load_benchmark_results.json in the current directory).
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
os.environ.setdefault("SQL_PATH", "postgresql+asyncpg://jobbers:jobbers@localhost:5432/jobbers")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
os.environ.setdefault("TASK_BACKEND", "sql")
os.environ.setdefault("DLQ_BACKEND", "sql")
os.environ.setdefault("TASK_SCHEDULER_BACKEND", "sql")
os.environ.setdefault("ROUTING_BACKEND", "sql")
os.environ.setdefault("CRON_DAG_SCHEDULER_BACKEND", "sql")

if "sqlite" in os.environ["SQL_PATH"]:
    raise SystemExit(
        "sql_load_benchmark.py requires a Postgres SQL_PATH -- SQLite's pool class "
        "(StaticPool) doesn't support the connection-pool tuning this measures, and "
        "serializes writers at the file level. See the module docstring."
    )

import asyncpg  # type: ignore[import-untyped]  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402
from sqlalchemy.exc import TimeoutError as SQLPoolTimeoutError  # noqa: E402
from ulid import ULID  # noqa: E402

from jobbers import db  # noqa: E402
from jobbers.models.queue_config import QueueConfig  # noqa: E402
from jobbers.models.task import Task  # noqa: E402
from jobbers.models.task_config import DeadLetterPolicy  # noqa: E402
from jobbers.registry import clear_registry, register_task  # noqa: E402
from jobbers.task_generator import TaskGenerator  # noqa: E402
from jobbers.task_processor import TaskProcessor  # noqa: E402

if TYPE_CHECKING:
    from jobbers.state_manager import StateManager

BENCH_TASK_NAME = "bench_noop"
BENCH_TASK_VERSION = 1
# Brief backoff when a poll-based pop finds nothing -- SQL has no blocking-pop primitive
# like Redis's BZPOPMIN, so a "no row available right now" result is a normal outcome
# under concurrent SELECT ... FOR UPDATE SKIP LOCKED contention, not an error.
_POLL_BACKOFF_SECS = 0.002


def _raw_dsn(sql_path: str) -> str:
    """Strip the +driver suffix and any query params (pool_size etc.) for raw asyncpg.connect(),
    which wants a plain postgresql://user:pass@host/db URL with no SQLAlchemy-only params."""
    url = make_url(sql_path).set(drivername="postgresql", query={})
    return url.render_as_string(hide_password=False)


def _dbname(sql_path: str) -> str:
    return make_url(sql_path).database or ""


@dataclass
class LevelResult:
    concurrency: int
    n_tasks: int
    sleep_ms: int
    fetchers: int
    pool_size: int
    max_overflow: int
    submit_seconds: float
    submit_ops_per_sec: float
    run_seconds: float
    throughput_tasks_per_sec: float
    pg_xact_per_sec: float
    pg_connections_baseline: int
    pg_connections_peak: int
    pg_db_size_delta_bytes: int
    latency_ms_p50: float
    latency_ms_p95: float
    latency_ms_p99: float
    latency_ms_max: float
    pool_timeout_error: bool = False
    tasks_completed: int = 0


@dataclass
class SweepResults:
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


async def _ensure_queue_and_role(sm: "StateManager", queue: str, role: str) -> None:
    await sm.routing.create_queue_config(QueueConfig(name=queue, max_concurrent=0))
    await sm.routing.create_role(role, {queue})


async def _submit_tasks(sm: "StateManager", queue: str, n: int, submit_concurrency: int) -> float:
    sem = asyncio.Semaphore(submit_concurrency)

    async def _one() -> None:
        async with sem:
            task = Task(id=ULID(), name=BENCH_TASK_NAME, version=BENCH_TASK_VERSION, queue=queue)
            await sm.submit_task(task)

    start = time.perf_counter()
    await asyncio.gather(*(_one() for _ in range(n)))
    return time.perf_counter() - start


async def _pg_stats(conn: "asyncpg.Connection", dbname: str) -> dict[str, Any]:
    connections = await conn.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname = $1", dbname)
    row = await conn.fetchrow(
        "SELECT xact_commit FROM pg_stat_database WHERE datname = $1", dbname
    )
    size = await conn.fetchval("SELECT pg_database_size($1)", dbname)
    return {
        "connections": connections or 0,
        "xact_commit": row["xact_commit"] if row else 0,
        "db_size_bytes": size or 0,
    }


async def _sample_peak_connections(
    conn: "asyncpg.Connection", dbname: str, peak: list[int], stop: asyncio.Event
) -> None:
    while not stop.is_set():
        try:
            count = await conn.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname = $1", dbname)
            peak[0] = max(peak[0], count or 0)
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            pass


async def _run_worker_loop(
    sm: "StateManager", role: str, n_tasks: int, concurrency: int, fetchers: int = 1
) -> tuple[list[float], bool]:
    """Drain up to n_tasks tasks with up to `concurrency` in flight, using `fetchers`
    concurrent pop loops (see scripts/redis_load_benchmark.py's _run_worker_loop for the
    parallel-fetcher rationale and the shared-queue-set fix for the RedisRoutingNotifications
    per-role pub/sub hazard -- that hazard applies here too, since routing notifications are
    always Redis-backed regardless of TASK_BACKEND).

    Unlike Redis's blocking BZPOPMIN, SQL's get_next_task (SELECT ... FOR UPDATE SKIP LOCKED)
    is non-blocking and can legitimately return None under concurrent contention even when
    tasks remain. The fetch budget is therefore decremented only on a *confirmed* successful
    pop, with a brief backoff (_POLL_BACKOFF_SECS) and retry on None -- decrementing
    before-the-attempt (as the Redis script does, safely, since its pop never returns None
    in a closed benchmark run) would let a poll-based backend exit early having "spent" its
    budget on unproductive polls, leaving tasks undrained.

    Returns (latencies_ms, hit_pool_timeout). If the SQLAlchemy connection pool is exhausted
    mid-run (sqlalchemy.exc.TimeoutError), stops early rather than crashing the whole sweep --
    that failure point is itself the data point we want.
    """
    coordinator = TaskGenerator(sm, role, max_tasks=0)
    queues = await coordinator.queues()

    semaphore = asyncio.Semaphore(concurrency)
    latencies_ms: list[float] = []
    lock = asyncio.Lock()
    hit_pool_timeout = False
    remaining = [n_tasks]
    active: list[asyncio.Task[None]] = []

    async def run_task(task: Task) -> None:
        nonlocal hit_pool_timeout
        popped_at = time.perf_counter()
        try:
            await TaskProcessor(sm).run(task)
        except SQLPoolTimeoutError:
            hit_pool_timeout = True
        finally:
            elapsed_ms = (time.perf_counter() - popped_at) * 1000
            async with lock:
                latencies_ms.append(elapsed_ms)
            semaphore.release()

    async def fetch_loop() -> None:
        nonlocal hit_pool_timeout
        while not hit_pool_timeout:
            if remaining[0] <= 0:
                return
            await semaphore.acquire()
            if hit_pool_timeout:
                semaphore.release()
                return
            try:
                task = await sm.get_next_task(queues)
            except SQLPoolTimeoutError:
                semaphore.release()
                hit_pool_timeout = True
                return
            if task is None:
                semaphore.release()
                await asyncio.sleep(_POLL_BACKOFF_SECS)
                continue
            remaining[0] -= 1  # only decrement on confirmed success -- see docstring
            t = asyncio.create_task(run_task(task))
            active.append(t)

    try:
        await asyncio.gather(*(fetch_loop() for _ in range(fetchers)))
    finally:
        if active:
            await asyncio.gather(*active, return_exceptions=True)

    return latencies_ms, hit_pool_timeout


async def run_level(
    stats_conn: "asyncpg.Connection",
    sampler_conn: "asyncpg.Connection",
    dbname: str,
    sm: "StateManager",
    concurrency: int,
    n_tasks: int,
    sleep_ms: int,
    submit_concurrency: int,
    pool_size: int,
    max_overflow: int,
    fetchers: int,
) -> LevelResult:
    queue = f"bench-{concurrency}-{sleep_ms}-{fetchers}"
    role = f"bench-role-{concurrency}-{sleep_ms}-{fetchers}"
    await _ensure_queue_and_role(sm, queue, role)

    submit_hit_pool_timeout = False
    try:
        submit_seconds = await _submit_tasks(sm, queue, n_tasks, submit_concurrency)
    except SQLPoolTimeoutError:
        submit_hit_pool_timeout = True
        submit_seconds = 0.0

    baseline = await _pg_stats(stats_conn, dbname)

    peak_connections = [baseline["connections"]]
    stop_event = asyncio.Event()
    sampler = asyncio.create_task(
        _sample_peak_connections(sampler_conn, dbname, peak_connections, stop_event)
    )

    start = time.perf_counter()
    latencies_ms: list[float] = []
    run_hit_pool_timeout = False
    if not submit_hit_pool_timeout:
        latencies_ms, run_hit_pool_timeout = await _run_worker_loop(
            sm, role, n_tasks, concurrency, fetchers
        )
    run_seconds = time.perf_counter() - start

    stop_event.set()
    await sampler

    final = await _pg_stats(stats_conn, dbname)

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
        fetchers=fetchers,
        pool_size=pool_size,
        max_overflow=max_overflow,
        submit_seconds=submit_seconds,
        submit_ops_per_sec=n_tasks / submit_seconds if submit_seconds else 0.0,
        run_seconds=run_seconds,
        throughput_tasks_per_sec=len(latencies_ms) / run_seconds if run_seconds else 0.0,
        pg_xact_per_sec=(final["xact_commit"] - baseline["xact_commit"]) / run_seconds
        if run_seconds
        else 0.0,
        pg_connections_baseline=baseline["connections"],
        pg_connections_peak=peak_connections[0],
        pg_db_size_delta_bytes=final["db_size_bytes"] - baseline["db_size_bytes"],
        latency_ms_p50=pct(0.50),
        latency_ms_p95=pct(0.95),
        latency_ms_p99=pct(0.99),
        latency_ms_max=latencies_ms[-1] if latencies_ms else 0.0,
        pool_timeout_error=submit_hit_pool_timeout or run_hit_pool_timeout,
        tasks_completed=len(latencies_ms),
    )


def _print_table(results: list[LevelResult]) -> None:
    header = (
        f"{'conc':>5} {'fetch':>5} {'sleep_ms':>8} {'done/n':>9} {'run_s':>7} {'tasks/s':>8} "
        f"{'pg_xact/s':>10} {'conns(base->peak)':>18} {'db_delta_mb':>12} "
        f"{'p50_ms':>7} {'p95_ms':>7} {'p99_ms':>7} {'poolerr':>8}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        conns = f"{r.pg_connections_baseline}->{r.pg_connections_peak}"
        done = f"{r.tasks_completed}/{r.n_tasks}"
        print(
            f"{r.concurrency:>5} {r.fetchers:>5} {r.sleep_ms:>8} {done:>9} {r.run_seconds:>7.2f} "
            f"{r.throughput_tasks_per_sec:>8.1f} {r.pg_xact_per_sec:>10.1f} {conns:>18} "
            f"{r.pg_db_size_delta_bytes / 1e6:>12.2f} "
            f"{r.latency_ms_p50:>7.1f} {r.latency_ms_p95:>7.1f} {r.latency_ms_p99:>7.1f} "
            f"{'ERROR' if r.pool_timeout_error else '-':>8}"
        )


async def main_async(args: argparse.Namespace) -> None:
    await _register_bench_task(args.sleep_ms)

    # --pool-size/--max-overflow/--pool-timeout are this script's own sweep parameters,
    # applied the same way jobbers/db.py supports tuning SQL_PATH in production: as query
    # params on the URL itself (?pool_size=...&max_overflow=...&pool_timeout=...), which
    # db.py pops off before constructing the engine (SQLAlchemy doesn't support this
    # natively -- see db.py and docs/benchmarking-and-performance.md). None means "use
    # the SQLAlchemy default" -- deliberately `is not None`, not truthiness: max_overflow=0
    # ("no overflow beyond pool_size") is a legitimate value a truthiness check would drop.
    pool_params: dict[str, str] = {}
    if args.pool_size is not None:
        pool_params["pool_size"] = str(args.pool_size)
    if args.max_overflow is not None:
        pool_params["max_overflow"] = str(args.max_overflow)
    if args.pool_timeout is not None:
        pool_params["pool_timeout"] = str(args.pool_timeout)
    if pool_params:
        url = make_url(os.environ["SQL_PATH"]).update_query_dict(pool_params)
        os.environ["SQL_PATH"] = url.render_as_string(hide_password=False)

    sm = await db.init_state_manager()
    engine = db._engine  # noqa: SLF001 -- benchmark tooling, reading pool config for reporting
    assert engine is not None  # noqa: S101
    effective_pool_size = engine.pool.size()  # type: ignore[attr-defined]
    effective_max_overflow = getattr(engine.pool, "_max_overflow", 0)

    sql_path = os.environ["SQL_PATH"]
    raw_dsn = _raw_dsn(sql_path)
    dbname = _dbname(sql_path)
    stats_conn = await asyncpg.connect(raw_dsn)
    sampler_conn = await asyncpg.connect(raw_dsn)

    concurrency_levels = [int(c) for c in args.concurrency.split(",")]

    results: list[LevelResult] = []
    try:
        for concurrency in concurrency_levels:
            n_tasks = args.tasks_per_level or max(500, concurrency * 20)
            n_tasks = min(n_tasks, args.max_tasks_per_level)
            submit_concurrency = min(concurrency, args.submit_concurrency)
            print(
                f"\n=== concurrency={concurrency} fetchers={args.fetchers} n_tasks={n_tasks} "
                f"sleep_ms={args.sleep_ms} pool_size={effective_pool_size} "
                f"max_overflow={effective_max_overflow} ==="
            )
            result = await run_level(
                stats_conn,
                sampler_conn,
                dbname,
                sm,
                concurrency,
                n_tasks,
                args.sleep_ms,
                submit_concurrency,
                effective_pool_size,
                effective_max_overflow,
                args.fetchers,
            )
            results.append(result)
            status = " *** sqlalchemy.exc.TimeoutError ***" if result.pool_timeout_error else ""
            print(
                f"  submit: {result.submit_seconds:.2f}s ({result.submit_ops_per_sec:.1f} tasks/s)  "
                f"run: {result.run_seconds:.2f}s ({result.tasks_completed}/{result.n_tasks} done, "
                f"{result.throughput_tasks_per_sec:.1f} tasks/s)  "
                f"pg: {result.pg_xact_per_sec:.1f} xact/s  "
                f"conns {result.pg_connections_baseline}->{result.pg_connections_peak}"
                f"{status}"
            )
            if result.pool_timeout_error:
                print(
                    f"  Hit pool timeout at concurrency={concurrency} "
                    f"(pool_size={effective_pool_size}, max_overflow={effective_max_overflow}); "
                    "stopping sweep here."
                )
                break

        print("\n" + "=" * 40 + " SUMMARY " + "=" * 40)
        print(f"pool_size={effective_pool_size} max_overflow={effective_max_overflow}")
        _print_table(results)

        sweep = SweepResults(levels=results)
        with open(args.out, "w") as f:
            json.dump(asdict(sweep), f, indent=2)
        print(f"\nWrote results to {args.out}")
    finally:
        await stats_conn.close()
        await sampler_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
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
        help="Simulated per-task work time in ms (0 = pure SQL-bound ceiling; try 50-200 for a "
        "more realistic light-task scenario).",
    )
    parser.add_argument(
        "--fetchers",
        type=int,
        default=1,
        help="Number of concurrent pop loops. 1 (default) mirrors worker_proc.main()'s single "
        "sequential fetch loop. Not swept automatically -- run separate invocations at "
        "different values to compare, same pattern as --pool-size.",
    )
    parser.add_argument(
        "--submit-concurrency",
        type=int,
        default=20,
        help="Concurrency used to seed each level's queue (kept independent of the worker "
        "concurrency under test so submission itself doesn't exhaust the pool; default 20).",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=None,
        help="SQLAlchemy engine pool_size for this benchmark run. Unset = SQLAlchemy "
        "default (5). 0 is a valid, meaningful value (no persistent pooled connections, "
        "all overflow) -- distinct from leaving this unset.",
    )
    parser.add_argument(
        "--max-overflow",
        type=int,
        default=None,
        help="SQLAlchemy engine max_overflow for this benchmark run. Unset = SQLAlchemy "
        "default (10). 0 is a valid, meaningful value (no bursting beyond pool_size) -- "
        "distinct from leaving this unset. Combined ceiling is pool_size + max_overflow.",
    )
    parser.add_argument(
        "--pool-timeout",
        type=float,
        default=None,
        help="Seconds to wait for a pool connection before sqlalchemy.exc.TimeoutError. "
        "Unset = SQLAlchemy default (30s) -- consider lowering for faster-failing sweeps.",
    )
    parser.add_argument(
        "--out",
        default="sql_load_benchmark_results.json",
        help="Path to write JSON results.",
    )
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
