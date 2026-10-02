# Changing Queue Configuration on a Running System

A task names its queue directly — there is no routing layer that can redirect it
after the fact. That makes queue changes simpler to reason about than they used to
be, and it moves the whole burden of "where does this work go" onto two levers:
**queue config** (capacity, rate limits) and **role membership** (which workers
consume which queues).

This guide is the playbook for changing either one without losing work.
[resource-management.md](resource-management.md) describes what the knobs *are*;
this describes how to turn them on a system that is already running.

> **Read §5 before deleting or renaming a queue.** Those are the only two operations
> here that can silently strand work, and nothing in the code currently stops you.

---

## 1. What can change live, and how fast

| Change | Live? | Propagates via | Worst-case latency |
| --- | --- | --- | --- |
| `max_concurrent` | Yes | `config:version` + role refresh tag | One `CONFIG_POLL_INTERVAL` (default 5s); workers, one fetch cycle |
| Rate limit fields | Yes | `config:version` | One `CONFIG_POLL_INTERVAL` per Manager replica |
| Role membership (`PUT /roles/{name}`) | Yes | Refresh tag + `queue-config-refresh:{role}` pub/sub | Effectively immediate; one fetch cycle if pub/sub is missed |
| Creating a queue | Yes | `config:version` | One `CONFIG_POLL_INTERVAL` |
| Deleting a queue | Yes, **destructive** | `config:version` + refresh tags | See §5 |
| Queue name | **No** | — | A rename is a delete plus a create (§5.3) |
| A task's queue, once submitted | **No** | — | Frozen at first submit; retries, scheduler dispatch and DLQ resubmit all reuse it |

Two mechanisms, not one, and they answer different questions:

- **`config:version`** is a single Redis key bumped by every queue-config write. Each
  process calls `refresh_config_if_stale()` on the paths that read config and drops
  its cache when the version moved. Throttled to `CONFIG_POLL_INTERVAL`.
- **Role refresh tags** answer "which queues do I poll?" and are pushed over pub/sub
  so an idle worker does not wait for a poll.

So a capacity change and a role change propagate by different routes and at
different speeds. If a change seems not to have taken effect, that distinction is
usually why.

---

## 2. The two ordering rules

**Create the queue before anything submits to it.** `validate_task` rejects a task
whose queue does not exist, and `get_queue_config` caches negative lookups. Queue
creation bumps `config:version`, which expires that cache, so a too-early probe
recovers within one `CONFIG_POLL_INTERVAL` rather than sticking forever — but
submissions in that window still fail.

**Put the queue in a role before work lands in it.** Queue existence is not
consumption. A queue that exists but belongs to no role accepts submissions and has
no consumer; worse, the Scheduler promotes due tasks only for **its own role's
queues** ([scheduler_proc.py](../jobbers/runners/scheduler_proc.py)), so retry-delayed
and scheduled tasks for an unpolled queue are never dispatched at all. Nothing warns
you.

---

## 3. Queue names

Queue names must match `^[a-zA-Z_][a-zA-Z0-9_]*$` — letters, digits and underscores,
not starting with a digit. **Hyphens are not allowed**: use `priority_shard_a`, not
`priority-shard-a`. The rule is the same identifier rule used for task and router
names, so that any queue can be named from a mermaid diagram label
(`task_name:queue_name`).

Enforced on create (the `QueueConfig.name` validator) and on update (the
`PUT /queues/{name}` path pattern). Role names are unconstrained — a role is never
named in a diagram.

---

## 4. Routine changes

### 4.1 Add a queue and start using it

```bash
POST /queues  { "name": "heavy_jobs", "max_concurrent": 3 }
PUT  /roles/default  { "queues": ["default", "heavy_jobs"] }
```

Then name it from a diagram (`extract:heavy_jobs`), a submit call
(`submit(queue="heavy_jobs")`) or a router's `RouteTo("task", queue="heavy_jobs")`.

Verify: `GET /queues` lists it, `GET /queues/default` shows it in the role, and
`tasks_selected{queue="heavy_jobs"}` starts incrementing once work arrives.

### 4.2 Change a concurrency cap

```bash
PUT /queues/external_api  { "name": "external_api", "max_concurrent": 5 }
```

**`max_concurrent` is per worker process, not global.** It is compared against an
in-memory count on each worker (`StateManager.current_tasks_by_queue`), so with ten
workers, `max_concurrent: 5` admits up to fifty concurrent tasks. To reason about a
global ceiling, multiply by your worker count — or give the queue its own role and
scale that deployment instead.

`0` and an explicit `null` both mean **unlimited**, not "blocked". Omitting the field
is different again: it defaults to `10`. There is no value of `max_concurrent` that
stops a queue.

### 4.3 Add or change a rate limit

```bash
PUT /queues/payment_gateway  {
  "name": "payment_gateway",
  "rate_numerator": 100,
  "rate_denominator": 1,
  "rate_period": "minute"
}
```

A rate limit **gates first submissions, not throughput.** Retries, requeues, DLQ
resubmits and fan-out arm batches all bypass the limiter. "100 per minute" therefore
means a hundred *admissions* per minute, and a queue under heavy retry load can
exceed it. If the point is to protect a downstream, a concurrency cap is the more
honest lever.

Rejected submissions surface as `TaskRateLimitedError` → HTTP 429. The caller is
expected to retry; nothing is queued.

### 4.4 Shift capacity to a hot queue

```bash
PUT /roles/default  { "queues": ["normal", "urgent"] }
```

Workers pick the new queue up on their next fetch cycle, no restart. This is the
fastest lever available and the one to reach for first in an incident: it moves
*capacity to the work* rather than work to capacity, which is the only direction
still possible now that queues are named directly.

### 4.5 Isolate a workload

```bash
POST /queues  { "name": "heavy_ml", "max_concurrent": 2 }
POST /roles   { "name": "ml_role", "queues": ["heavy_ml"] }
# deploy workers with WORKER_ROLE=ml_role
```

Give a queue its own role the moment you want a separate worker pool, a separate
failure domain, or to scale or pause it independently.

---

## 5. Destructive changes

### 5.1 Drain a queue

"Drain" means *stop consuming while in-flight work finishes* — it does **not** stop
submissions:

```bash
PUT /roles/default  { "queues": ["other_queue"] }   # drop draining_queue
```

Workers stop polling `draining_queue` on their next fetch cycle. In-flight tasks run
to completion and are not cancelled.

**Submissions keep arriving and keep succeeding.** The queue still exists, so
`validate_task` accepts it, and anything naming that queue — a diagram label, a cron
entry, a retry already scheduled — piles up unconsumed. There is currently **no way
to pause a queue**: `max_concurrent: 0` means unlimited, and the rate-limit fields
are truthy-checked so `0` reads as "no limit". To stop work arriving you have to stop
the producers, or change what they name, which is a deploy.

So a drain is one half of a retirement, and the order matters:

1. Stop producers naming the queue (deploy, or edit the cron entries that use it).
2. Remove the queue from every role so nothing new starts.
3. Wait for in-flight work to finish, or let it finish while consumers remain.
4. Check all four hiding places (§5.2) are empty.
5. Only then delete.

### 5.2 Before you delete a queue, check four places

Deleting a queue removes its config and its role memberships. It does **not** move or
report work still associated with it, and the Scheduler and Cleaner both enumerate
their work from `get_all_queues()` — so once the queue leaves the registry, anything
left is invisible rather than merely stranded.

| Where work hides | Check | What a non-empty result means |
| --- | --- | --- |
| Queued, waiting | `GET /task-list?queue=X&status=submitted` | Enqueued tasks that will never be popped |
| Scheduled / retry-delayed | `GET /scheduled-tasks?queue=X` | **The invisible half.** These live in a per-queue sorted set that the Scheduler finds via `get_all_queues()`; after deletion they are never dispatched, never listed, and `recover_orphans` cannot see them |
| In flight | `GET /active-tasks?queue=X` | Tasks running right now with live heartbeats |
| Dead letter | `GET /dead-letter-queue?queue=X` | Resubmitting these later re-enqueues to the deleted queue |

```bash
DELETE /queues/X   # 404 if it does not exist; no other guard
```

`DELETE /queues/{name}` returns 404 for an unknown queue and otherwise always
succeeds. **It does not check any of the four.** A delete guard (409 unless
`?force=true`) is a known gap — see
[.claude/plans/lane-as-primitive.md](../.claude/plans/lane-as-primitive.md) §6.3.

Scheduled tasks are the ones that catch people out, because they can be hours in the
future under exponential backoff. A queue whose active set is empty can still have a
schedule set full of retries.

### 5.3 Do not rename a queue

There is no rename. A queue's name is part of its tasks' persisted state and part of
the Redis key of its queue, heartbeat, schedule and rate-limiter sets, so renaming is
a delete plus a create — with every hazard in §5.2, plus the fact that
already-submitted tasks keep the old name and become unroutable.

To move a workload to a differently-named queue:

1. Create the new queue and add it to the role **alongside** the old one.
2. Change producers to name the new queue (deploy; update cron entries).
3. Let the old queue drain — it still has consumers, so in-flight, queued **and
   scheduled** work all complete normally.
4. Remove the old queue from the role and verify §5.2's four checks.
5. Delete the old queue.

Running both queues in one role through the transition is what makes this safe:
nothing is stranded because nothing stops being consumed.

---

## 6. Verifying a change landed

| Question | How to answer |
| --- | --- |
| Did the config write take? | `GET /queues/{name}/config` — but note it is served from the answering replica's cache, refreshed on the same throttle |
| Have workers noticed a role change? | `queue_config_refreshes{role}` increments; `refresh_lag_ms{role}` records the lag between the tag bump and pickup |
| Have processes dropped stale config? | `config_refreshes` increments once per process per observed version change |
| Is the queue actually being consumed? | `tasks_selected{queue}` increments; `time_in_queue{queue}` shows wait time |
| Is anything still on the old queue? | The four checks in §5.2 |

To force a role refresh rather than wait:

```bash
POST /roles/{role_name}/refresh
```

This works for every `ROUTING_BACKEND`, including `static` — it touches only the
Redis-backed refresh-tag and pub/sub mechanism. On `static` it is pointless but
harmless: the config never changes, so workers get back what they already had.

---

## 7. Known sharp edges

Stated plainly, because each one is a way to lose work and none is currently guarded:

- **No pause.** Nothing stops submissions to an existing queue (§5.1).
- **No delete guard.** `DELETE /queues/{name}` does not check for queued, scheduled,
  in-flight or dead-lettered work (§5.2).
- **A queue in no role is a black hole**, and its scheduled tasks never dispatch at
  all (§2).
- **`max_concurrent` is per worker**, so the global ceiling is `cap × workers` (§4.2).
- **Rate limits gate admissions, not throughput** — retries and DLQ resubmits bypass
  them (§4.3).
- **A task's queue is frozen at first submit.** Changing configuration never moves
  work that already exists; there is no re-resolve or bulk-move operation.
- **On `sql` + SQLite, the rate limiter is racy.** `SELECT FOR UPDATE` is a no-op, so
  concurrent submitters can exceed the limit. Use PostgreSQL for multi-worker
  deployments.

---

## See also

- [resource-management.md](resource-management.md) — what each control does, and how
  they compose
- [operations.md](operations.md) — running the four processes, refresh mechanics,
  metrics
- [routing-backend-feature-matrix.md](routing-backend-feature-matrix.md) — where
  queue and role config is stored per `ROUTING_BACKEND`
