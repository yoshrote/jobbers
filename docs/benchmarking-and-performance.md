# Benchmarking and Performance

How much load a Jobbers worker generates on its storage backend as concurrency scales, what that implies for `WORKER_CONCURRENT_TASKS` and connection/server settings, and how to reproduce or extend the numbers yourself.

Two companion scripts, one per backend family:

- [`scripts/redis_load_benchmark.py`](../scripts/redis_load_benchmark.py) — the `redis_json` backend (`TASK_BACKEND=redis_json`, `DLQ_BACKEND=redis_json`, `ROUTING_BACKEND=redis_json`), the default and most common production configuration. The general shape of its findings (one `BZPOPMIN` in flight per worker process, connection-pool sizing) applies to the plain `redis` backend too, but exact round-trip counts differ; see [task-backend-feature-matrix.md](task-backend-feature-matrix.md) for backend differences.
- [`scripts/sql_load_benchmark.py`](../scripts/sql_load_benchmark.py) — the `sql` backend (`TASK_BACKEND=sql`, `DLQ_BACKEND=sql`, `TASK_SCHEDULER_BACKEND=sql`, `ROUTING_BACKEND=sql`) against **PostgreSQL only**. SQLite is deliberately out of scope for this benchmark — its pool class rejects the connection-pool tuning being measured, and it serializes writers at the file level, so it can't represent the concurrency this exercises. MySQL is untested territory for this codebase (see the [SQL backend](#sql-backend-postgresql) section) and not covered here.

Redis- and SQL-specific findings are in their own sections below, each ending with its own "using the benchmark script" walkthrough. For the short answer to "what do I actually set `WORKER_CONCURRENT_TASKS` to," skip to [Summary: sizing `WORKER_CONCURRENT_TASKS`](#summary-sizing-worker_concurrent_tasks) at the end.

---

## Test methodology

The benchmark does not synthesize Redis traffic from first principles — it drives the **real** worker code path, `TaskGenerator` + `TaskProcessor`, exactly what `runners/worker_proc.py` runs in production, against a live Redis Stack instance. For each concurrency level in a sweep it:

1. Registers a real task (`bench_noop`, with a configurable `--sleep-ms` of simulated work) via the normal `@register_task` decorator.
2. Creates a scratch queue/role pair (`max_concurrent=0`, i.e. unlimited) via the real routing backend.
3. Submits `n` tasks through `StateManager.submit_task` — the same call `TaskWrapper.submit()` makes.
4. Runs a worker loop structurally identical to `worker_proc.main()`: an `asyncio.Semaphore(concurrency)` gates how many `TaskProcessor(sm).run(task)` calls are in flight, while a **single sequential fetch loop** pulls the next task via `anext(TaskGenerator)`.
5. Samples Redis `INFO` before and after (command counts, `connected_clients`, `used_memory`) and records per-task latency (pop → completion).

Because it reuses production code (not a mock), the round-trip counts, atomic-pipeline behavior, and connection-pool interactions it measures are exactly what a real worker does — the only thing synthetic is the task body itself (`asyncio.sleep(sleep_ms / 1000)` instead of real work).

**Environment for the numbers below:** Redis Stack (`redis/redis-stack-server`) in Docker on the same host as the benchmark process (loopback network, sub-millisecond RTT). This is a best-case network path — see [Extrapolating to your environment](#extrapolating-to-your-environment) before trusting these numbers for a real deployment.

---

## Redis round-trips per task lifecycle stage

Counted from the `redis_json` adapters (`adapters/redis_json/*.py`) in atomic-pipeline mode (the mode used whenever task state, scheduler, and DLQ share one Redis instance — see [datastore-architecture.md](datastore-architecture.md)):

| Stage | Round trips | Detail |
| --- | --- | --- |
| Submit (steady state) | 1 | Single Lua script: `EXISTS` + `ZADD` (queue) + `JSON.SET` (blob) + `SADD`/`SREM` (type index), atomically |
| Worker pop | 3 | `BZPOPMIN` (blocking) + `JSON.GET` (blob) + `ZSCORE` (heartbeat) |
| Heartbeat set | 1 | `ZADD` on the heartbeat sorted set, once per task at start |
| Heartbeat remove | 1 | `ZREM`, once per task at end |
| Completion (success) | ~3 | `WATCH` + `JSON.GET` (read-for-watch) + `MULTI`/`JSON.SET`/`EXEC` (one pipelined round trip) |
| Immediate retry | 1 | One `MULTI/EXEC` pipeline: `ZADD` (queue) + `JSON.SET` (blob) + index update |
| Dead-letter save | ~3 | Same WATCH/GET/MULTI shape as completion, plus a DLQ `JSON.SET` folded into the same transaction |

A task that runs once and succeeds costs the worker roughly **8 round trips**: 3 to pop, 2 for heartbeat set/remove, 3 to complete. `RediSearch` (`FT.SEARCH`) queries are not on this path at all — they're used only for admin/listing/DLQ-browsing/routing-CRUD, so they don't factor into per-task worker throughput.

---

## Findings

### 1. A single worker process has a hard throughput ceiling — set by its own architecture, not by Redis

`TaskGenerator.__anext__` fetches tasks strictly sequentially: `worker_proc.main()`'s loop `await`s the current `anext(task_generator)` call before starting the next one, so there is **exactly one `BZPOPMIN` in flight per worker process**, no matter how high `WORKER_CONCURRENT_TASKS` is set. Concurrency only bounds how many already-popped tasks *execute* at once — it does not parallelize popping.

Near-zero-work tasks (`--sleep-ms 0`, isolating the pop-loop/Redis-bound ceiling), 1500 tasks/level:

| concurrency | tasks/s | redis ops/s | connections (base→peak) |
| ---: | ---: | ---: | --- |
| 1 | 207 | 3,729 | 1→3 |
| 5 | 481 | 8,667 | 6→6 |
| 25 | 475 | 8,560 | 23→23 |
| 100 | 484 | 8,733 | 25→25 |
| 300 | 486 | 8,759 | 28→28 |

Throughput plateaus almost immediately past concurrency≈5 — raising `WORKER_CONCURRENT_TASKS` further does nothing for a single process, because the sequential pop loop is already the bottleneck. Redis itself was barely touched: ~8,700 ops/sec against a backend capable of tens of thousands for these command types. **The bottleneck is the worker's own single-fetch-loop design, not Redis capacity.**

### 2. Realistic (non-zero) task duration scales concurrency near-linearly — up to the same ceiling

Tasks with 50ms of simulated work (`--sleep-ms 50`), 1000 tasks/level:

| concurrency | tasks/s | connections (base→peak) |
| ---: | ---: | --- |
| 1 | 16.7 | 1→3 |
| 10 | 153.5 | 12→14 |
| 25 | 306.4 | 23→30 |
| 50 | 424.5 | 30→30 |
| 100 | 426.8 | 30→30 |
| 200 | 446.6 | 30→30 |

Throughput scales near-linearly with concurrency up to ~50, then hits the **same ~450-490 tasks/sec ceiling** as the zero-work sweep — concurrency beyond that point buys nothing for this process, because the pop loop can no longer feed the semaphore fast enough to keep it full.

**Takeaway:** size `WORKER_CONCURRENT_TASKS` to your actual task duration; once you hit the per-process ceiling, add more worker processes rather than raising concurrency further.

### 3. redis-py's connection pool defaults to 100 connections per process — not unbounded

`jobbers/db.py` builds its Redis client via `redis.from_url(...)` without an explicit `max_connections`. It is easy to assume this means unbounded, but redis-py's async `ConnectionPool` resolves an unset `max_connections` to a **hard default of 100**, not `2**31`/unbounded. Every Jobbers process (manager, each worker, scheduler, cleaner) gets its own independent 100-connection ceiling.

Reproduced directly: a burst of 150 simultaneous Redis calls (150 concurrent task submissions/completions) against the default pool:

```text
redis.exceptions.MaxConnectionsError: Too many connections
```

Retried with `--max-connections 300` (or, in production, `REDIS_URL="...?max_connections=300"`) — succeeded, same ~470 tasks/sec ceiling as before, 150 connections opened and held:

| concurrency | max_connections | result | tasks/s | connections (base→peak) |
| ---: | ---: | --- | ---: | --- |
| 150 | 100 (default) | `MaxConnectionsError` | — | — |
| 150 | 300 | success | 468.5 | 150→150 |

**Connections are never reclaimed** — redis-py's pool has no idle-connection GC; it only grows to its historical peak simultaneous demand and stays there for the life of the process. A single burst (a synchronized retry storm, a fleet-wide restart, a backlog catch-up after downtime) can permanently inflate a process's connection count until it restarts. Also note connections aren't held for a task's full duration — redis-py returns a connection to the pool after each command completes (except inside a `WATCH`/`MULTI` block) — so the connection count you need is driven by **how many Redis commands can land at the exact same instant**, not by steady-state concurrency. This is why the 50ms-task sweep above needed only ~30 connections at concurrency=200: individual commands are fast, so the odds of many landing simultaneously stay low outside of bursts.

### 4. Parallel fetch loops were prototyped and rejected — more worker processes is the better lever

Since Finding 1 pins the per-process ceiling on the single sequential `BZPOPMIN` fetch loop, the natural next question is whether running multiple concurrent fetch loops within one process raises that ceiling. An earlier version of `redis_load_benchmark.py` prototyped exactly this (a `--fetchers N` flag); it has since been **removed from the script**, but the findings below are kept as the record of why.

**A correctness hazard surfaced immediately.** The naive version — `N` independent `TaskGenerator` instances, each calling `.queues()` every iteration like `worker_proc.main()` does — crashed:

```text
RuntimeError: readuntil() called while another coroutine is already waiting for incoming data
```

`TaskGenerator.queues()` calls `StateManager.poll_refresh_signal(role)`, which reads from `RedisRoutingNotifications`'s **single cached pub/sub connection per role**. Two fetch loops for the same role polling that one connection concurrently race on the same underlying socket reader. This is a real, currently-dormant hazard: today it can't fire because every worker process only ever runs one `TaskGenerator`, but it would fire immediately for anyone implementing multiple concurrent fetchers per process without addressing it — worth keeping in mind if this is ever revisited.

**Even after working around that hazard** (by resolving the queue set once and sharing it read-only across fetch loops, rather than each loop polling independently), the throughput gain was modest and came at a real latency cost. At concurrency=25, zero-work tasks, 1500 tasks/level:

| fetchers | tasks/s | redis ops/s | p50 / p95 / p99 latency (ms) |
| ---: | ---: | ---: | --- |
| 1 | 470.9 | 8,490 | 3.3 / 4.7 / 5.5 |
| 2 | 558.6 | 9,509 | 9.1 / 11.2 / 12.2 |
| 4 | 580.9 | 9,890 | 18.1 / 21.6 / 27.0 |
| 8 | 572.5 | 9,754 | 29.1 / 39.1 / 47.0 |

Throughput gained **~19% going from 1→2 fetchers, another ~4% from 2→4, then flattened (and slightly regressed) from 4→8**. Meanwhile **p99 latency grew roughly linearly with fetcher count** (~6-9ms per additional fetcher) — each additional fetcher is more concurrent work competing for the same event loop and connection pool, without a proportional throughput return past 2-4. Redis-side ops/sec moved only slightly (~8,500 → ~9,900, still far from Redis's real ceiling), confirming the added fetchers mostly bought *local* scheduling parallelism, not additional Redis capacity.

**Takeaway: running additional worker processes is significantly the better proposition.** It scales throughput linearly, requires no code changes, and introduces none of the shared-connection contention or latency growth this experiment surfaced. Parallel fetch loops are not implemented in `worker_proc.py` and are not planned — if a single process's ceiling genuinely isn't enough, add processes rather than revisiting this.

---

## Extrapolating to your environment

These numbers were measured on loopback (Redis Stack in Docker, same host, sub-millisecond RTT) — a best-case network path. Two things change under real-world RTT to Redis:

- **The per-process throughput ceiling scales down roughly with RTT.** The pop cycle is ~4 sequential round trips (routing-version check, `BZPOPMIN`, `JSON.GET`, `ZSCORE`); at ~0.1-0.3ms RTT (this benchmark's environment) that's the ~470-490 tasks/sec ceiling measured above. At a more typical same-AZ RTT of ~0.5-1ms, expect proportionally lower — roughly `measured_ceiling × (measured_RTT / your_RTT)`. Cross-AZ or cross-region RTT (multiple ms) will lower it further; don't put a worker fleet in a different region from its Redis.
- **Connections are held longer per command**, so the same burst size needs proportionally fewer *simultaneous* tasks to threaten the pool ceiling. Re-run the burst test (§ below) against your actual network path if connection exhaustion is a concern.

**Run this benchmark against Redis on your actual production network path** before trusting either number for capacity planning — localhost results don't transfer 1:1.

---

## Recommendations

### `WORKER_CONCURRENT_TASKS`

- Size it to your actual task duration: `concurrency ≈ target_throughput_per_process × avg_task_duration_seconds`. Setting it far above that only holds idle Redis connections for no throughput benefit (see Finding 1).
- Measure your real per-process ceiling with this benchmark against Redis on your production network path (see [Extrapolating to your environment](#extrapolating-to-your-environment)) — don't assume the loopback numbers above.
- Once you hit that per-process ceiling, **scale by adding worker processes/replicas**, not by raising concurrency further. Redis has enormous headroom left at the measured ceiling (~8,700 ops/sec against a backend capable of far more); a worker fleet is far more likely to be bottlenecked by the tasks' own external I/O or CPU than by this access pattern.
- If a per-process ceiling above ~500 tasks/sec is genuinely needed, **add worker processes** — that's the recommended lever. Parallel fetch loops within one process were prototyped and rejected (see [Finding 4](#4-parallel-fetch-loops-were-prototyped-and-rejected--more-worker-processes-is-the-better-lever)): the throughput gain was modest, it introduced a real correctness hazard (a shared per-role pub/sub connection), and it is not implemented in `worker_proc.py`.

### Redis connection tuning

- Tune `max_connections` — and any other redis-py connection setting — as a **query parameter on `REDIS_URL`**, e.g. `REDIS_URL="redis://host:6379?max_connections=300"`. No code change is needed; redis-py's `from_url()` parses these and they take precedence over any explicit kwarg. This is the mechanism used throughout Jobbers for connection tuning.
- Size `max_connections` for your **worst burst**, not steady state — a rough floor is `WORKER_CONCURRENT_TASKS + 15-20%` headroom, since connections are never reclaimed once opened (Finding 3).
- Every Jobbers process opens its own independent pool. Make sure Redis's own `maxclients` (default 10000) comfortably covers the **sum across your whole fleet** (manager + N workers + scheduler + cleaner).
- Don't override `socket_timeout` — `jobbers/db.py` deliberately leaves it `None` to avoid racing `BZPOPMIN`'s server-side infinite block (a documented redis-py 8.0 regression).

### Redis server configuration

- **`maxmemory-policy` must be `noeviction`** (Redis's own default — don't change it). Jobbers treats task JSON docs, queue/heartbeat sorted sets, and RediSearch indexes as authoritative state, not a cache; an eviction policy would silently corrupt in-flight task/queue data. Pair with a `maxmemory` ceiling so a full instance fails loud (write errors) instead of eventually OOMing the host, and monitor `used_memory` with headroom.
- **Enable AOF** (`appendonly yes`, `appendfsync everysec` is a reasonable default) rather than relying on RDB snapshots alone — Redis is the system of record here; RDB-only persistence can lose minutes of queue/task state on a crash.
- Completed tasks are **not deleted by default** (`cleanup_on=None` on `@register_task`) — memory grows linearly with task volume unless you set `cleanup_on` on your task configs or rely on the Cleaner's age-based pruning (`--completed-task-age`; see [operations.md#cleaner](operations.md#cleaner)). Confirm this is actually configured before production volume.
- Run a **dedicated** Redis Stack instance, not shared with unrelated caching traffic — Jobbers issues blocking `BZPOPMIN` and `WATCH`/`MULTI` transactions from every worker; noisy-neighbor latency directly hurts the atomic-dispatch retry path.
- **Use `redis/redis-stack-server` (or `redis-stack`), not plain `redis:latest`**, for the `redis_json` backend — plain Redis lacks the RedisJSON/RediSearch modules that `ensure_index()`/`JSON.SET` require at startup. Worth double-checking your deployment configs actually do this, since it's easy to get this wrong when `TASK_BACKEND` defaults to `redis_json`.
- If you ever consider Redis Cluster for horizontal scaling: Jobbers' Lua-script atomicity (submit, atomic dispatch, etc. — see [datastore-architecture.md](datastore-architecture.md)) requires the task blob and queue keys to be co-located on the same node; cross-slot scripts would break. A single well-resourced primary + replica is the safer path unless you're prepared to redesign key hashing.

---

## Using the benchmark script

[`scripts/redis_load_benchmark.py`](../scripts/redis_load_benchmark.py) requires a Redis Stack instance (RedisJSON + RediSearch) — plain `redis:latest` will not work, since it exercises the `redis_json` backend end to end. Point it at a **throwaway** instance; it creates/deletes scratch queues, roles, and real task blobs.

```bash
# spin up a scratch Redis Stack instance
docker run -d --name jobbers-bench-redis -p 16379:6379 redis/redis-stack-server:latest

# basic sweep
REDIS_URL=redis://localhost:16379 python scripts/redis_load_benchmark.py \
    --concurrency 1,5,10,25,50,100,200 --tasks-per-level 2000

# add simulated per-task work time to move off the pure-Redis-bound ceiling
REDIS_URL=redis://localhost:16379 python scripts/redis_load_benchmark.py \
    --concurrency 5,25,100 --sleep-ms 50

# probe the connection-pool ceiling directly
REDIS_URL=redis://localhost:16379 python scripts/redis_load_benchmark.py \
    --concurrency 150 --submit-concurrency 150 --tasks-per-level 300 --max-connections 300
```

### CLI reference

| Flag | Default | Meaning |
| --- | --- | --- |
| `--concurrency` | `1,5,10,25,50,100,200` | Comma-separated `WORKER_CONCURRENT_TASKS`-equivalent levels to sweep. |
| `--tasks-per-level` | `0` (auto: `max(500, concurrency*20)`) | Tasks to process at each level. |
| `--max-tasks-per-level` | `5000` | Cap applied even when auto-sizing. |
| `--sleep-ms` | `0` | Simulated per-task work time. `0` isolates the pop-loop/Redis-bound ceiling; `50`-`200` approximates a light real task. |
| `--submit-concurrency` | `20` | Concurrency used to *seed* each level's queue, kept independent of the worker concurrency under test so submission itself doesn't exhaust the pool. Raise this deliberately (e.g. to match `--concurrency`) to test a submission burst. |
| `--max-connections` | `0` (library default: 100) | This script's own `max_connections`, as a distinct flag so it's easy to vary across a sweep. Production tuning uses a `REDIS_URL` query param instead (see [Redis connection tuning](#redis-connection-tuning)) — this flag exists for benchmarking convenience, not as a new production knob. |
| `--out` | `redis_load_benchmark_results.json` | Path to write JSON results. |

### Interpreting the output table

```text
 conc sleep_ms    done/n   run_s  tasks/s redis_ops/s  clients(base->peak) mem_delta_mb  p50_ms  p95_ms  p99_ms  maxconn
```

| Column | Meaning |
| --- | --- |
| `conc` | The `WORKER_CONCURRENT_TASKS`-equivalent level for this row. |
| `done/n` | Tasks completed vs. requested. Less than `n` means the run stopped early — check `maxconn`. |
| `run_s` | Wall-clock time for the worker loop to drain this level's queue. |
| `tasks/s` | End-to-end throughput as seen by the worker loop (`done / run_s`). This is the number to compare against your target throughput when sizing `WORKER_CONCURRENT_TASKS`. |
| `redis_ops/s` | Redis-side command throughput, from `INFO`'s `total_commands_processed` delta over `run_s`. Compare against Redis's real capacity (tens of thousands+ for simple ops) to judge how much headroom is left — in every sweep here it stayed far below Redis's ceiling, confirming the worker loop (not Redis) is the bottleneck. |
| `clients(base->peak)` | `connected_clients` on the Redis server immediately before this level started, and the peak observed during it. A `peak` well below `maxconn`/pool size means you have headroom; `peak` equal to your configured `max_connections` combined with a `maxconn` error means you found the ceiling. |
| `mem_delta_mb` | `used_memory` delta over the run. Noisy at small task counts/blob sizes (Redis allocator behavior) — trust the trend across levels, not a single row, and use larger `--tasks-per-level` for a cleaner signal if you need to project memory growth. |
| `p50_ms` / `p95_ms` / `p99_ms` | Per-task latency from pop to completion. At `--sleep-ms 0` this is pure Redis+scheduling overhead; at higher `--sleep-ms` it should track close to `sleep_ms` plus that same overhead — a p99 much higher than p50 suggests contention (e.g. `WATCH`/`MULTI` retries under concurrent writes to the same task, or connection-pool waits). |
| `maxconn` | `ERROR` if this level hit `MaxConnectionsError` (during submission or the worker loop). The sweep stops at the first level that errors — later levels are not attempted, since a higher concurrency is only more likely to fail the same way. |

### JSON output format

`--out` writes a `SweepResults` object: `{"levels": [...]}`, one entry per concurrency level with every column above plus the raw fields it's derived from (`submit_seconds`, `submit_ops_per_sec`, `redis_connected_clients_baseline`/`_peak`, `redis_used_memory_delta_bytes`, `max_connections_error`, `tasks_completed`). Useful for diffing two sweeps (e.g. before/after a `max_connections` change, or comparing environments) without re-parsing the printed table.

### Extending it

The script is intentionally a thin driver over real `StateManager`/`TaskGenerator`/`TaskProcessor` calls, not a bespoke load generator — to test a different scenario, the natural extension points are `_register_bench_task` (swap in a task body closer to your real workload) and the `--concurrency`/`--sleep-ms`/`--max-connections` sweep dimensions already exposed. Keep any new dimension you want to sweep as its own CLI flag (matching the existing pattern) rather than folding it into an env var, so scripted sweeps stay simple.

If you're tempted to reintroduce parallel fetch loops within one process, read [Finding 4](#4-parallel-fetch-loops-were-prototyped-and-rejected--more-worker-processes-is-the-better-lever) first — it documents a real correctness hazard (a per-role pub/sub connection shared across concurrent fetchers) and modest-at-best throughput gains. Adding worker processes is the recommended way to scale past a single process's ceiling.

---

## SQL backend (PostgreSQL)

Everything in this section is `TASK_BACKEND=sql` (paired with `DLQ_BACKEND=sql`, `TASK_SCHEDULER_BACKEND=sql`, `ROUTING_BACKEND=sql`) against **PostgreSQL**, measured with [`scripts/sql_load_benchmark.py`](../scripts/sql_load_benchmark.py) — the same methodology as the Redis benchmark above (drives real `StateManager`/`TaskGenerator`/`TaskProcessor` code, not synthetic traffic), adapted for SQL's different pop semantics and connection-pool model.

**Why not SQLite:** SQLite's pool class (`StaticPool`) rejects `pool_size`/`max_overflow` outright, and SQLite serializes writers at the file level — it can't represent the multi-connection concurrency this benchmark measures. **Why not MySQL:** untested anywhere in this codebase. The SQL adapters' comments explicitly reason about Postgres's READ COMMITTED isolation semantics for the concurrency-sensitive paths (fan-in, atomic dispatch, rate limiting); MySQL's different default isolation level (REPEATABLE READ) means those same code paths would need independent correctness verification, not just a performance run, before trusting results there.

**Architectural note — Redis is still required.** Even with every `*_BACKEND` set to `"sql"`, a Jobbers deployment still needs a running Redis instance: `StateManager`'s cancellation bus and routing-refresh notifications (`RedisCancellationBus` / `RedisRoutingNotifications`) are unconditionally Redis-backed in `db.py`'s `init_state_manager()`, regardless of `TASK_BACKEND`. A plain `redis:latest` is sufficient for this path — Redis Stack/RedisJSON is not required. There is currently no all-SQL, Redis-free deployment mode.

### Local setup

`docker-compose.yaml` includes a `postgres` service (added alongside `redis` for local benchmarking/testing — not wired as the default `SQL_PATH` for the `manager`/`worker`/`scheduler` services, which still default to SQLite unless you override `SQL_PATH` yourself):

```bash
docker compose up -d postgres redis
pip install -e ".[postgres]"   # installs asyncpg, not a default dependency
```

### Redis round-trips vs. SQL statements per task lifecycle stage

SQL has no single-round-trip atomic primitive analogous to a Redis Lua script — each stage is one or more SQL statements inside one transaction (`BEGIN` + statements + `COMMIT`, all against the same checked-out connection). Compare against the [redis_json round-trip table](#redis-round-trips-per-task-lifecycle-stage) above:

| Stage | Statements (in one txn) | Detail |
| --- | --- | --- |
| Submit (new task) | ~4 | Upsert `tasks` (`UPDATE` then `INSERT` since the row doesn't exist yet) + upsert `task_queue` (same `UPDATE`-then-`INSERT` pattern) |
| Worker pop | 3 | `SELECT ... FOR UPDATE SKIP LOCKED` (find oldest queued id) + `DELETE FROM task_queue` + `SELECT` the task row |
| Heartbeat set | 1 | `UPDATE tasks SET heartbeat_at = ...` |
| Heartbeat remove | 1 | `UPDATE tasks SET heartbeat_at = NULL` |
| Completion (success) | 1 | `UPDATE tasks ... WHERE status = 'STARTED'` — the compare-and-set is a single `WHERE`-guarded `UPDATE`, no separate read-then-write step |
| Immediate retry | ~4 | Same upsert shape as submit |

Notably, SQL's compare-and-set operations (completion, `compare_and_set_status`) are **cheaper** than Redis's — a single `UPDATE ... WHERE status = ?` is atomic by construction, with no `WATCH`/`MULTI`/retry-on-conflict dance needed (Postgres's row lock handles it). Submit is **more expensive** than Redis's single-Lua-script submit, since there's no equivalent to bundling multiple statements into one round trip.

### Findings

**1. Same fetch-loop-bound ceiling pattern as Redis, but roughly 3x lower.** Zero-work tasks (`--sleep-ms 0`), 500 tasks/level, default pool (`pool_size=5`, `max_overflow=10`):

| concurrency | tasks/s | pg xact/s | connections (base→peak) |
| ---: | ---: | ---: | --- |
| 1 | 50.4 | 265 | 3→3 |
| 5 | 152.4 | 706 | 7→7 |
| 15 | 150.7 | 630 | 7→7 |
| 50 | 154.4 | 629 | 7→7 |

Throughput plateaus past concurrency≈5 at **~150 tasks/sec/process** — the same single-sequential-fetch-loop architecture as Redis (Finding 1 above), just capped lower because each SQL round trip costs more than each Redis round trip locally (see Finding 3). Peak connections stayed at 7 through concurrency=50 — well under the 15-connection default pool, for the same reason as Redis: connections are held only for the duration of each transaction, not the task's full lifetime, so realistic per-task SQL traffic doesn't naturally pile up connections at moderate concurrency.

**2. The default connection pool is far tighter than Redis's.** SQLAlchemy's async engine defaults to `pool_size=5` + `max_overflow=10` — a **15-connection ceiling per process**, versus redis-py's 100-connection default. Unlike `REDIS_URL`, this isn't natively a query-string-tunable DBAPI setting — `pool_size`/`max_overflow`/`pool_timeout` are SQLAlchemy engine-construction kwargs, and passing them as raw URL query params fails outright (SQLAlchemy forwards unrecognized query params straight to the DBAPI driver's `connect()` call; asyncpg raises `TypeError: connect() got an unexpected keyword argument 'pool_size'`). `jobbers/db.py` didn't expose a way to tune them at all before this benchmark. It now supports the same `?pool_size=...&max_overflow=...&pool_timeout=...` convention on `SQL_PATH` that `REDIS_URL` supports for redis-py — implemented by popping the recognized params off the URL itself before constructing the engine, so the DBAPI driver never sees them (non-SQLite `SQL_PATH` only — SQLite's `StaticPool` rejects these kwargs outright).

**3. SQLAlchemy's pool degrades gracefully (queues), while redis-py's fails fast.** This is a meaningful behavioral difference, not just a numbers difference. redis-py's pool raises `MaxConnectionsError` **immediately** when the pool is full. SQLAlchemy's pool instead **blocks the caller**, queuing up to `pool_timeout` (default 30s) waiting for a connection to free up, only raising `sqlalchemy.exc.TimeoutError` if that elapses. In practice this means a Postgres connection-pool bottleneck shows up first as **added latency** (silent queueing), not an error — which can mask the problem until something else times out first (an HTTP request deadline on the Manager API, for instance), or until a sustained-enough burst finally exceeds `pool_timeout` itself. Demonstrated directly: realistic jobbers-sized bursts (up to 200 concurrent submits against the 15-connection default) never triggered `TimeoutError` at all — individual round trips are fast enough locally that connections cycle back through the pool before 200 "concurrent" Python coroutines create genuine simultaneous demand. Reproducing the failure required either an artificially tiny pool (`--pool-size 1 --max-overflow 0`) or a deliberately short `--pool-timeout`.

**4. Parallel fetch loops were also prototyped here, and helped even less than for Redis, at a much steeper latency cost.** Like the Redis benchmark, an earlier version of `sql_load_benchmark.py` had a `--fetchers N` flag; it has since been **removed**, but the data is kept as the record of why. At concurrency=25, zero-work tasks:

| fetchers | pool_size / max_overflow | tasks/s | pg xact/s | p50 / p95 / p99 latency (ms) |
| ---: | --- | ---: | ---: | --- |
| 1 | 5 / 10 (default) | 148.1 | 656 | 16.4 / 22.0 / 28.4 |
| 8 | 20 / 20 | 178.2 | 828 | 88.1 / 123.3 / 204.7 |

+20% throughput for a **~5.4x p50 / ~7.2x p99 latency increase** — a markedly worse tradeoff than Redis's parallel-fetch-loop experiment (Redis Finding 4: +19% throughput for a much smaller latency cost). `SELECT ... FOR UPDATE SKIP LOCKED` contention against the same small `task_queue` table under concurrent fetchers costs more here than Redis's lock-free sorted-set pop. **Takeaway: additional worker processes is significantly the better proposition for SQL too, more so than for Redis** — parallel fetch loops are not implemented in `worker_proc.py` and are not planned.

**5. `synchronous_commit` materially affects both throughput and latency.** Postgres defaults to `synchronous_commit = on` — every `COMMIT` waits for a WAL fsync. Disabling it for a test run (`ALTER SYSTEM SET synchronous_commit = off; SELECT pg_reload_conf();`) at concurrency=5-25, zero-work tasks:

| `synchronous_commit` | tasks/s | p50 / p99 latency (ms) |
| --- | ---: | --- |
| `on` (default) | ~150 | 16.4 / 28.4 |
| `off` | ~197 | 12.4 / 15-17 |

**+30-33% throughput, and p99 dropped to near p50** (less variance — no more waiting on fsync completion). This is a real, standard Postgres tuning lever, not a Jobbers-specific one — but it trades a small durability window (the last few committed transactions may not survive an OS/Postgres crash, though the database itself stays consistent; no corruption risk) for throughput and latency. Whether that trade is acceptable depends on how you'd want a worker restart to behave if a handful of just-completed tasks' terminal status were lost — the Cleaner's stale-task detection would likely just re-flag them as stalled rather than silently losing them, but verify that matches your tolerance before enabling it in production.

### Recommendations

- **Install `asyncpg`** (`pip install -e ".[postgres]"`) and point `SQL_PATH` at Postgres for any multi-worker deployment — this was already documented guidance; this benchmark confirms SQLite would not represent real concurrent behavior.
- **Tune the connection pool explicitly** via `?pool_size=...&max_overflow=...&pool_timeout=...` on `SQL_PATH` — the same convention as `REDIS_URL`, and no code change needed. The SQLAlchemy defaults (5 / 10 / 30s) are conservative. Size `pool_size + max_overflow` per process the same way as Redis's `max_connections` (Finding 3 in the Redis section): headroom over `WORKER_CONCURRENT_TASKS`, summed across your whole fleet against Postgres's own `max_connections` (defaults to 100 server-side — the same number as redis-py's client-side default, coincidentally).
- **Don't rely on pool exhaustion to reveal itself as an error.** Because SQLAlchemy queues rather than fails fast, watch p95/p99 task latency (already emitted via the `task_execution_time`/`task_end_to_end_latency` OTel metrics — see [operations.md](operations.md#opentelemetry-metrics)) for creeping growth as a leading indicator, rather than waiting for `TimeoutError` to show up in logs.
- **Add worker processes** if one process's ~150 tasks/sec ceiling (this environment's number — re-measure on your network path) isn't enough. Parallel fetch loops within one process were prototyped and rejected for this backend too, at a steeper latency cost than for Redis (Finding 4).
- **Consider `synchronous_commit = off`** only after confirming the durability tradeoff (Finding 5) is acceptable for your task volume and recovery expectations — it's a genuine throughput/latency win, not a Jobbers-specific hack, but it's a deliberate choice to make explicitly, not a default to flip blindly.
- **Postgres server config, same principles as the Redis section:** run a dedicated instance if possible (row-lock contention on `task_queue`/`tasks` under concurrent workers is sensitive to noisy-neighbor I/O latency); ensure `max_connections` covers your fleet's summed pool ceilings; standard Postgres operational practices (autovacuum tuning, WAL sizing) apply as they would to any moderate-write-volume OLTP workload — nothing Jobbers-specific beyond what's covered above.

### Using the SQL benchmark script

```bash
docker compose up -d postgres redis
pip install -e ".[postgres]"

# basic sweep
SQL_PATH="postgresql+asyncpg://jobbers:jobbers@localhost:5432/jobbers" \
REDIS_URL=redis://localhost:6379 python scripts/sql_load_benchmark.py \
    --concurrency 1,5,10,25,50,100 --tasks-per-level 1500

# probe the connection-pool ceiling directly (SQLAlchemy queues rather than fails fast --
# see Finding 3 -- so a short --pool-timeout is needed to see the failure quickly)
SQL_PATH="..." REDIS_URL=... python scripts/sql_load_benchmark.py \
    --concurrency 20 --submit-concurrency 20 --pool-size 1 --max-overflow 0 --pool-timeout 0.01
```

Default credentials/DSN match the `postgres` service added to `docker-compose.yaml`: user/password/db all `jobbers`, port `5432`.

#### CLI reference

Mirrors `redis_load_benchmark.py`'s flags (`--concurrency`, `--tasks-per-level`, `--max-tasks-per-level`, `--sleep-ms`, `--submit-concurrency`, `--out`) with SQL-specific additions:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--pool-size` | unset (SQLAlchemy default: 5) | This run's `pool_size`, applied as a `SQL_PATH` query param (same mechanism as production tuning — see Finding 2). `0` is a valid, meaningful value (no persistent pooled connections) — distinct from leaving it unset. |
| `--max-overflow` | unset (SQLAlchemy default: 10) | This run's `max_overflow`, applied the same way. `0` is a valid, meaningful value (no bursting beyond `pool_size`) — distinct from leaving it unset. Combined ceiling is `pool_size + max_overflow`. |
| `--pool-timeout` | unset (SQLAlchemy default: 30s) | This run's `pool_timeout`, in seconds, applied the same way. Lower it to make a pool-exhaustion sweep fail fast instead of waiting up to 30s per level (see Finding 3). |

#### Interpreting the output table

```text
 conc sleep_ms    done/n   run_s  tasks/s  pg_xact/s  conns(base->peak)  db_delta_mb  p50_ms  p95_ms  p99_ms  poolerr
```

Same shape as the Redis table, with SQL-flavored columns: `pg_xact/s` is `pg_stat_database.xact_commit` delta over the run (the closest Postgres analog to Redis's `total_commands_processed` — each transaction bundles one stage's statements, similar to how a Lua script bundles Redis's). `conns(base->peak)` comes from `pg_stat_activity` filtered to the target database, sampled the same way as Redis's `connected_clients`. `db_delta_mb` is `pg_database_size()` delta — noisy at small task counts, same caveat as Redis's `mem_delta_mb`. `poolerr` is `ERROR` on `sqlalchemy.exc.TimeoutError` (during submission or the worker loop); the sweep stops at the first level that errors.

One measurement quirk: `pg_xact/s` can read `0` (or, on an error-terminated row, a meaningless huge number from dividing by a near-zero elapsed time) on very small/fast runs — Postgres's statistics view has a brief refresh lag relative to the query that reads it. Use `--tasks-per-level` of at least a few hundred for a reliable reading; treat any row flagged `ERROR` as having no meaningful timing/throughput columns regardless of what they show.

#### Extending it

Same philosophy as the Redis script: a thin driver over real `StateManager`/`TaskGenerator`/`TaskProcessor` calls. As with the Redis script, parallel fetch loops within one process were prototyped and rejected here (Finding 4) — the same `RedisRoutingNotifications` per-role pub/sub hazard applies, since routing notifications are always Redis-backed regardless of `TASK_BACKEND` (see the architectural note at the top of this section). Add worker processes instead of revisiting that.

---

## Summary: sizing `WORKER_CONCURRENT_TASKS`

Both backends follow the same underlying model, so one formula covers both:

```text
WORKER_CONCURRENT_TASKS ≈ min(target_throughput_per_process, measured_ceiling) × avg_task_duration_seconds
```

**Why this formula.** `WORKER_CONCURRENT_TASKS` doesn't set throughput directly — it sets how many already-popped tasks can be executing at once (Redis Finding 1 / SQL Finding 1). By Little's Law, sustaining a given throughput with a given per-task duration requires that many concurrent slots: to hold 100 tasks/sec in flight at 200ms each, you need `100 × 0.2 = 20` slots. But no amount of concurrency lets one process exceed its **measured per-process ceiling** — the sequential fetch loop's own rate (~470-490 tasks/sec for Redis, ~150 tasks/sec for SQL, *in this benchmark's environment*; re-measure on your production network path per [Extrapolating to your environment](#extrapolating-to-your-environment)). Once `concurrency × (1 / avg_task_duration)` reaches that ceiling, more concurrency just holds idle connections for no additional throughput (Redis Finding 1, SQL Finding 1) — so the formula's `min()` caps the useful range.

**Practical range:** treat the formula's output as a floor, not an exact target — real task durations vary, and a semaphore that's just barely large enough will occasionally sit idle waiting on the next pop between tasks finishing. Multiply by **1.5-2x** for headroom against that variance; there's little downside to modest over-provisioning (Finding 1 in both sections shows throughput plateaus rather than degrading once you're past the saturation point), but under-provisioning leaves real throughput on the table. Don't go below **~5** even for near-instant tasks — the default (`WORKER_CONCURRENT_TASKS=5`) is already a reasonable floor for that case, since sub-millisecond tasks don't benefit from more concurrency regardless (Redis Finding 1's zero-work sweep plateaued by concurrency≈5).

**Worked examples**, assuming you want to saturate one process's full ceiling (i.e. `target_throughput_per_process = measured_ceiling` — see below if your target is lower than the ceiling), using this benchmark's measured ceilings (re-measure your own before trusting these for capacity planning):

| avg task duration | Redis: concurrency (×1.5 headroom) | SQL: concurrency (×1.5 headroom) |
| --- | ---: | ---: |
| ~0ms (near-instant) | 5 (floor) | 5 (floor) |
| 10ms | ~7 | 5 (floor) |
| 50ms | ~36 | ~11 |
| 200ms | ~144 | ~45 |
| 1s | ~720 | ~225 |

The 1s row is arithmetically what "saturate the ceiling with 1-second tasks" requires, but treat triple-digit-plus concurrency as a signal to sanity-check assumptions rather than a number to deploy blindly — this benchmark measured Jobbers' own bookkeeping overhead, not per-task memory/thread/external-API-rate-limit costs, and those are far more likely to constrain a process running hundreds of genuinely-1-second tasks concurrently than anything measured here.

**If your target throughput is lower than the ceiling** (the common case — most deployments don't need to saturate a single process), skip the table and use `target_throughput_per_process × avg_task_duration_seconds × 1.5`, floor 5, directly — no need to reach the ceiling at all.

**When the target exceeds one process's ceiling** (`target_throughput_per_process > measured_ceiling`, regardless of task duration): don't raise `WORKER_CONCURRENT_TASKS` past the ceiling-saturating value above — set concurrency to saturate that ceiling and **add more worker processes/replicas** for the rest of the needed throughput (Redis Finding 1 / SQL Finding 1's primary recommendation). Redis has far more headroom to scale into per process than SQL (~470-490 vs. ~150 tasks/sec here) — an SQL-backed deployment will need proportionally more worker processes for the same aggregate throughput target.

**Parallel fetch loops are not part of this formula, and not a lever available at all today.** Both benchmark scripts previously prototyped running multiple concurrent fetch loops within a single process (a `--fetchers` flag) as a way to squeeze more out of one process; that prototype has since been removed from both scripts. It surfaced a real correctness hazard (a per-role pub/sub connection shared across concurrent fetchers) and delivered only modest throughput for a real latency cost — worse for SQL than Redis (Redis Finding 4's +19% throughput for a moderate latency cost vs. SQL Finding 4's +20% for a ~5-7x latency cost). **Running additional worker processes is significantly the better proposition**: it scales linearly, needs no code changes, and avoids that contention entirely — reach for it first, and don't reintroduce parallel fetch loops without re-reading Finding 4 in both sections.

**Once concurrency is set, size the connection pool from it**, not the other way around: `max_connections` (Redis, via `?max_connections=...` on `REDIS_URL`) or `pool_size + max_overflow` (SQL, via the same convention on `SQL_PATH`) should be `WORKER_CONCURRENT_TASKS` plus ~15-20% headroom for bursts, per Redis Finding 3 / SQL Finding 2-3 — summed across your whole fleet against the server's own connection ceiling (Redis's `maxclients`, Postgres's `max_connections`).
