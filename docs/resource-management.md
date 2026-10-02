# Resource Management in Jobbers

Jobbers provides four complementary mechanisms for controlling how computing resources are consumed and how live traffic is directed across workers. They compose: a task that is individually rate-limited will still compete for the worker's global concurrency slot.

---

## 1. Worker Concurrency

The coarsest knob. Each worker process has a fixed pool of **task slots** controlled by a single asyncio semaphore.

```bash
WORKER_CONCURRENT_TASKS=5   # default
```

The worker acquires a slot before fetching the next task and releases it when execution finishes. If all slots are occupied the fetch loop simply waits — no busy-polling, no dropped tasks.

| Env Variable | Default | Effect |
| --- | --- | --- |
| `WORKER_CONCURRENT_TASKS` | `5` | Semaphore size; maximum simultaneously executing tasks per worker process |
| `WORKER_TTL` | `50` | Worker restarts itself after processing this many tasks (0 = infinite). Useful for bounding memory growth in long-running workers |
| `WORKER_ROLE` | `"default"` | Which role (set of queues) this worker consumes |

**When to use:** Scale a worker up by raising `WORKER_CONCURRENT_TASKS` when tasks are I/O-bound and the worker has headroom. Scale it down (or run fewer worker containers) to shed overall load.

---

## 2. Queue Configuration

Queues are the primary unit of traffic control. Each queue has two independent resource controls stored in the routing backend (SQL, Redis, or static, depending on `ROUTING_BACKEND`) and enforced at task-fetch time.

### Per-Queue Concurrency Cap

```python
# POST /queues  or  PUT /queues/{name}
{
  "name": "heavy_jobs",
  "max_concurrent": 3
}
```

No more than `max_concurrent` tasks from this queue will run at the same time across a single worker. The `TaskGenerator` checks the worker's live per-queue active count before offering that queue for the next fetch. A queue at its cap is temporarily excluded from the round; it re-enters as soon as a slot opens.

`max_concurrent: 0` or an explicit `max_concurrent: null` both mean **unlimited** concurrency for that queue — not "block this queue." Omitting the field entirely is different: `QueueConfig.max_concurrent` defaults to `10`, a real cap, not unlimited — you have to pass `null` explicitly to get unlimited. Negative values are rejected at the API/model level.

This is enforced in memory by `StateManager.current_tasks_by_queue` — a dict updated atomically when tasks start and finish via the `task_in_registry()` context manager.

### Per-Queue Rate Limiting

```python
{
  "name": "external_api_calls",
  "rate_numerator": 10,
  "rate_denominator": 1,
  "rate_period": "minute"    # "second" | "minute" | "hour" | "day"
}
```

Interpreted as: **10 tasks every 1 minute**. The rate period in seconds is `rate_denominator × period_unit`.

Rate limiting is enforced at **submission time** and is supported by `redis`, `redis_json`, and `sql` (`TASK_BACKEND`); the mechanism differs per backend but the windowing semantics are identical.

On `redis`/`redis_json`, an atomic Lua script on a Redis sorted set (`rate-limiter:{queue}`):

1. Removes entries older than the rate window.
2. Counts remaining entries.
3. Enqueues the task only if `count < rate_numerator`; otherwise returns 0 (task not enqueued).

Because the check and enqueue happen in a single Lua transaction, there are no race conditions under concurrent submissions.

On `sql`, `SQLTaskSubmit.submit_rate_limited_task` reproduces the same sliding window with two tables: `rate_limit_entries` (one row per task_id currently inside the window) and `rate_limit_anchors` (one row per queue, locked via `SELECT ... FOR UPDATE` to serialize concurrent submitters to that queue). On PostgreSQL the anchor lock makes the check-then-enqueue atomic, matching the Redis guarantee; on SQLite `FOR UPDATE` is a no-op, so concurrent submitters to the same queue can race past the limit check — same caveat as SQLite's general multi-worker unsafety (see [task-backend-feature-matrix.md](task-backend-feature-matrix.md)).

The Cleaner process periodically prunes stale entries from rate-limiter sorted sets (`redis`/`redis_json`) or the `rate_limit_entries` table (`sql`).

### Propagating a Config Change

Queue configs are cached per process, so a write has to be announced. Every write bumps one Redis key, `config:version`, and every process that reads queue config calls `StateManager.refresh_config_if_stale()`, which drops its cache when the version has moved.

The check is throttled to `CONFIG_POLL_INTERVAL` seconds (default 5) so it can sit directly on the paths that read config:

| Caller | Cadence |
| --- | --- |
| `StateManager.submit_task` | Throttled — the one place every submit reads queue config |
| `validate_task` | Throttled — this is where a stale *negative* lookup would reject a valid queue |
| `GET /queues/{name}/config` | Throttled — so the admin UI does not show another replica's stale view |
| `TaskGenerator.queues()` | Every iteration (`min_interval=0`) |

So the worst case for a config change to take effect anywhere is one `CONFIG_POLL_INTERVAL`, and the `config_refreshes` counter records each time a process drops its cache.

The case this exists for: `get_queue_config` caches negative lookups, and the `in`-check makes them sticky. A process that probed a queue name before the queue was created would otherwise reject every submission to it until restart. Queue creation bumps the version, which clears the entry.

Still, the ordering advice costs nothing: **create the queue before anything submits to it.**

### Queue CRUD API

| Method | Endpoint | Effect |
| --- | --- | --- |
| `GET` | `/queues` | List all queues |
| `POST` | `/queues` | Create queue (409 if already exists) |
| `PUT` | `/queues/{name}` | Create or update queue config (upsert) |
| `GET` | `/queues/{name}/config` | Fetch current config |
| `DELETE` | `/queues/{name}` | Delete queue; cascades to role mappings and bumps affected roles' refresh tags |

**Queue names** must match `^[a-zA-Z_][a-zA-Z0-9_]*$` — letters, digits and underscores, not starting with a digit. **Hyphens are not allowed**: use `priority_shard_a`, not `priority-shard-a`. The rule is the same identifier rule used for task and router names, so that any queue can be named from a mermaid diagram label. It is enforced on create (the `QueueConfig.name` validator) and on update (the `PUT /queues/{name}` path pattern); role names are unconstrained, since a role is never named in a diagram.

---

## 3. Task Configuration

Per-task-type resource controls are set at registration time via `@register_task` and baked into the worker binary. The resource-relevant parameters are:

- `timeout` — hard execution timeout (seconds); task is cancelled if exceeded
- `max_concurrent` — per-task-type concurrency cap across all queues on this worker
- `max_heartbeat_interval` — if a task misses its heartbeat window the Cleaner marks it `STALLED`, freeing its concurrency slot
- `on_shutdown` — what happens to in-flight tasks on SIGTERM: `STOP` (→ STALLED), `RESUBMIT` (re-enqueued), or `CONTINUE` (shielded to completion)
- `dead_letter_policy` — `NONE` (failed tasks stay in FAILED state) or `SAVE` (copied to the Dead Letter Queue for inspection and replay)

See [task-definition-reference.md](task-definition-reference.md) for the full parameter reference, retry/backoff formulas, and heartbeat details.

---

## 4. Live Traffic Direction: Dynamic Roles and Queue Remapping

> For the step-by-step playbooks — creating, draining, retiring and replacing a queue on a running system, and what to check before deleting one — see [queue-operations.md](queue-operations.md). This section covers the mechanism; that one covers the procedure.

Roles are named sets of queues. A worker consumes exactly the queues belonging to its role, determined by `WORKER_ROLE`. Roles and their queue memberships can be changed **without restarting workers**.

### Role CRUD API

| Method | Endpoint | Effect |
| --- | --- | --- |
| `GET` | `/roles` | List all roles |
| `POST` | `/roles` | Create role with initial queue list |
| `GET` | `/roles/{name}` | Fetch queues for role |
| `PUT` | `/roles/{name}` | Replace queue list for role; triggers live refresh |
| `DELETE` | `/roles/{name}` | Delete role (queues are preserved) |

### The Refresh Tag Mechanism

Each role has a `refresh_tag` (a ULID). Workers poll the tag on each fetch cycle and re-query their queue list whenever it changes; a Redis pub/sub channel (`queue-config-refresh:{role}`) delivers immediate notification so idle workers don't wait for the next poll. The tag is bumped on every queue or role mutation, including queue deletion.

See [operations.md — Queue configuration refresh](operations.md#queue-configuration-refresh) for the full mechanics, propagation latency details, and observability metrics.

### Traffic Direction Patterns

#### Drain a queue gradually

Stop *consuming* a queue while in-flight tasks complete naturally:

```bash
# Remove the queue from the role that processes it
PUT /roles/default  { "queues": ["other_queue"] }
# Workers stop polling "draining_queue" on their next fetch cycle
# In-flight tasks on "draining_queue" finish normally (not cancelled)
```

This does **not** stop submissions — the queue still exists, so `validate_task` accepts it and new work piles up unconsumed. There is no way to pause a queue. Draining is one half of retiring one: see [queue-operations.md § 5](queue-operations.md#5-destructive-changes) for the full sequence and the four places work hides before it is safe to delete.

#### Shift capacity to a hot queue

```bash
# Add a high-priority queue to a role
PUT /roles/default  { "queues": ["normal", "urgent"] }
# Workers pick up "urgent" on their next fetch cycle, no restart needed
```

#### Isolate a task type to dedicated workers

```bash
# Create an isolated queue and role
POST /queues  { "name": "heavy_ml", "max_concurrent": 2 }
POST /roles   { "name": "ml-role", "queues": ["heavy_ml"] }
# Deploy workers with WORKER_ROLE=ml-role
```

#### Throttle a queue under load

```bash
# Apply a concurrency cap without touching workers
PUT /queues/external_api  { "name": "external_api", "max_concurrent": 5 }
# Takes effect on next task-fetch cycle
```

Remember this cap is **per worker process** — the global ceiling is `max_concurrent × worker count`.

#### Emergency rate limit

```bash
# Add a rate limit to an overloaded downstream
PUT /queues/payment_gateway  {
  "name": "payment_gateway",
  "rate_numerator": 100,
  "rate_denominator": 1,
  "rate_period": "minute"
}
# New submissions are rejected at the rate limit boundary immediately
```

This gates *first submissions*, not throughput: retries, requeues, DLQ resubmits and fan-out arm batches bypass the limiter.

---

## Interaction Between Controls

The controls form a layered pipeline. A task must clear every layer to run:

```text
Submission time
  └─ Queue rate limit (atomic on redis/redis_json and sql+PostgreSQL; racy on sql+SQLite)
        └─ Task enqueued (SUBMITTED)

Fetch time (per worker)
  └─ Worker semaphore slots available?
        └─ Queue concurrency cap not reached? (StateManager.current_tasks_by_queue)
              └─ Task dequeued and started (STARTED)

Execution time
  └─ Task-level timeout (asyncio.wait_for)
        └─ Heartbeat monitored by Cleaner
              └─ on_shutdown policy on SIGTERM
```

A conservative deployment approach is to set `max_concurrent` on queues to express your intent about concurrency per queue, and use `WORKER_CONCURRENT_TASKS` to express the total capacity of a worker pod — the queue caps then subdivide that capacity across workload types.
