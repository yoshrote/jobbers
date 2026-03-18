# Resource Management in Jobbers

Jobbers provides four complementary mechanisms for controlling how computing resources are consumed and how live traffic is directed across workers. They compose: a task that is individually rate-limited will still compete for the worker's global concurrency slot.

---

## 1. Worker Concurrency

The coarsest knob. Each worker process has a fixed pool of **task slots** controlled by a single asyncio semaphore.

```
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

Queues are the primary unit of traffic control. Each queue has two independent resource controls stored in SQL and enforced at task-fetch time.

### Per-Queue Concurrency Cap

```python
# POST /queues  or  PUT /queues/{name}
{
  "name": "heavy-jobs",
  "max_concurrent": 3
}
```

No more than `max_concurrent` tasks from this queue will run at the same time across a single worker. The `TaskGenerator` checks the worker's live per-queue active count before offering that queue for the next fetch. A queue at its cap is temporarily excluded from the round; it re-enters as soon as a slot opens.

This is enforced in memory by `StateManager.current_tasks_by_queue` — a dict updated atomically when tasks start and finish via the `task_in_registry()` context manager.

### Per-Queue Rate Limiting

```python
{
  "name": "external-api-calls",
  "rate_numerator": 10,
  "rate_denominator": 1,
  "rate_period": "minute"    # "second" | "minute" | "hour" | "day"
}
```

Interpreted as: **10 tasks every 1 minute**. The rate period in seconds is `rate_denominator × period_unit`.

Rate limiting is enforced at **submission time** via an atomic Lua script on a Redis sorted set (`rate-limiter:{queue}`). The script:

1. Removes entries older than the rate window.
2. Counts remaining entries.
3. Enqueues the task only if `count < rate_numerator`; otherwise returns 0 (task not enqueued).

Because the check and enqueue happen in a single Lua transaction, there are no race conditions under concurrent submissions.

The Cleaner process periodically prunes stale entries from rate-limiter sorted sets.

### Queue CRUD API

| Method | Endpoint | Effect |
| --- | --- | --- |
| `GET` | `/queues` | List all queues |
| `POST` | `/queues` | Create queue (409 if already exists) |
| `PUT` | `/queues/{name}` | Create or update queue config (upsert) |
| `GET` | `/queues/{name}/config` | Fetch current config |
| `DELETE` | `/queues/{name}` | Delete queue; cascades to role mappings and bumps affected roles' refresh tags |

---

## 3. Task Configuration

Per-task-type resource controls are set at registration time and baked into the worker binary. They govern retry behaviour, execution time, and what happens on worker shutdown.

```python
@register_task(
    name="my_task",
    version=1,
    max_retries=5,          # total retry attempts before giving up
    retry_delay=10,         # base delay in seconds before retry
    backoff_strategy=BackoffStrategy.EXPONENTIAL,
    max_retry_delay=3600,   # cap on computed delay, seconds
    timeout=300,            # hard execution timeout, seconds
    max_concurrent=1,       # per-task-type concurrency limit (across all queues)
    max_heartbeat_interval=dt.timedelta(minutes=5),
    dead_letter_policy=DeadLetterPolicy.SAVE,
    on_shutdown=TaskShutdownPolicy.STOP,
)
async def my_task(**kwargs):
    ...
```

### Retry and Backoff

When a task fails and has retries remaining the retry delay is computed from the backoff strategy:

| Strategy | Formula |
| --- | --- |
| `CONSTANT` | `retry_delay` |
| `LINEAR` | `retry_delay × attempt` |
| `EXPONENTIAL` | `retry_delay × 2^attempt` |
| `EXPONENTIAL_JITTER` | `uniform(0, retry_delay × 2^attempt)` |

The result is capped at `max_retry_delay`. If `retry_delay` is set the task transitions to `SCHEDULED` and the Scheduler re-queues it when it comes due; otherwise it is immediately re-enqueued as `UNSUBMITTED`.

### Heartbeat Monitoring

Long-running tasks should call `task.heartbeat()` periodically. If `max_heartbeat_interval` is set and a task misses its heartbeat window, the Cleaner marks it `STALLED`. This prevents zombie tasks from holding queue concurrency slots indefinitely.

### Shutdown Policy

Controls what happens to in-flight tasks when a worker receives SIGTERM:

| Policy | Behaviour |
| --- | --- |
| `STOP` | Cancel immediately; task moves to `STALLED` |
| `RESUBMIT` | Re-enqueue as `UNSUBMITTED` (another worker picks it up) |
| `CONTINUE` | Shield with `asyncio.shield()`; task runs to completion before the worker exits |

### Dead Letter Policy

| Policy | Behaviour |
| --- | --- |
| `NONE` | Failed tasks (retries exhausted) are simply marked `FAILED` |
| `SAVE` | Failed tasks are copied to the Dead Letter Queue for inspection and manual replay |

---

## 4. Live Traffic Direction: Dynamic Roles and Queue Remapping

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

Each role has a `refresh_tag` (a ULID) stored in SQL. `TaskGenerator` caches the tag it last saw. On each task fetch cycle it re-reads the tag (at most once per `config_ttl` seconds, default 60 s). If the tag has changed the generator re-queries its queue list and rate/concurrency limits before the next fetch.

```
PUT /roles/gpu-workers  { "queues": ["ml-inference", "image-resize"] }
  → generates new ULID refresh_tag in SQL

Worker polling loop:
  → reads refresh_tag → mismatch → re-fetches queue list → now consuming ml-inference + image-resize
```

The refresh tag is also bumped whenever a queue is deleted, ensuring workers that included that queue automatically drop it within one `config_ttl` window.

### Traffic Direction Patterns

#### Drain a queue gradually

Stop new submissions to a queue while in-flight tasks complete naturally:

```bash
# Remove the queue from the role that processes it
PUT /roles/default  { "queues": ["other-queue"] }
# Workers stop polling "draining-queue" within config_ttl seconds
# In-flight tasks on "draining-queue" finish normally (not cancelled)
```

#### Shift capacity to a hot queue

```bash
# Add a high-priority queue to a role
PUT /roles/default  { "queues": ["normal", "urgent"] }
# Workers pick up "urgent" within config_ttl seconds, no restart needed
```

#### Isolate a task type to dedicated workers

```bash
# Create an isolated queue and role
POST /queues  { "name": "heavy-ml", "max_concurrent": 2 }
POST /roles   { "name": "ml-role", "queues": ["heavy-ml"] }
# Deploy workers with WORKER_ROLE=ml-role
```

#### Throttle a queue under load

```bash
# Apply a concurrency cap without touching workers
PUT /queues/external-api  { "name": "external-api", "max_concurrent": 5 }
# Takes effect on next task-fetch cycle
```

#### Emergency rate limit

```bash
# Add a rate limit to an overloaded downstream
PUT /queues/payment-gateway  {
  "name": "payment-gateway",
  "rate_numerator": 100,
  "rate_denominator": 1,
  "rate_period": "minute"
}
# New submissions are rejected at the rate limit boundary immediately
```

---

## Interaction Between Controls

The controls form a layered pipeline. A task must clear every layer to run:

```
Submission time
  └─ Queue rate limit (Redis Lua, atomic)
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
