# Benchmarking and Performance

How much Redis load a Jobbers worker generates as concurrency scales, what that implies for `WORKER_CONCURRENT_TASKS` and Redis connection/server settings, and how to reproduce or extend the numbers yourself with [`scripts/redis_load_benchmark.py`](../scripts/redis_load_benchmark.py).

Scope: this document covers the `redis_json` backend (`TASK_BACKEND=redis_json`, `DLQ_BACKEND=redis_json`, `ROUTING_BACKEND=redis_json`) — the default and the most common production configuration. The general shape of the findings (one `BZPOPMIN` in flight per worker process, connection-pool sizing) applies to the plain `redis` backend too, but exact round-trip counts differ; see [task-backend-feature-matrix.md](task-backend-feature-matrix.md) for backend differences.

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

```
redis.exceptions.MaxConnectionsError: Too many connections
```

Retried with `--max-connections 300` (or, in production, `REDIS_URL="...?max_connections=300"`) — succeeded, same ~470 tasks/sec ceiling as before, 150 connections opened and held:

| concurrency | max_connections | result | tasks/s | connections (base→peak) |
| ---: | ---: | --- | ---: | --- |
| 150 | 100 (default) | `MaxConnectionsError` | — | — |
| 150 | 300 | success | 468.5 | 150→150 |

**Connections are never reclaimed** — redis-py's pool has no idle-connection GC; it only grows to its historical peak simultaneous demand and stays there for the life of the process. A single burst (a synchronized retry storm, a fleet-wide restart, a backlog catch-up after downtime) can permanently inflate a process's connection count until it restarts. Also note connections aren't held for a task's full duration — redis-py returns a connection to the pool after each command completes (except inside a `WATCH`/`MULTI` block) — so the connection count you need is driven by **how many Redis commands can land at the exact same instant**, not by steady-state concurrency. This is why the 50ms-task sweep above needed only ~30 connections at concurrency=200: individual commands are fast, so the odds of many landing simultaneously stay low outside of bursts.

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

### Redis connection tuning

- Tune `max_connections` — and any other redis-py connection setting — as a **query parameter on `REDIS_URL`**, e.g. `REDIS_URL="redis://host:6379?max_connections=300"`. No code change is needed; redis-py's `from_url()` parses these and they take precedence over any explicit kwarg. This is the mechanism used throughout Jobbers for connection tuning — don't add a separate env var for it.
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

```
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
