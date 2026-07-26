# Task & DAG Design Guide

Short version: **a Task is the unit of retry.** Everything in this guide follows
from that one fact. Decide how to split work into Tasks by asking "if this
piece re-runs alone, is that safe?" — not by code-organization instincts like
file size or function length.

For mechanics (how to wire a DAG, full config field reference, resource
knobs), see [dag-composition.md](dag-composition.md),
[task-definition-reference.md](task-definition-reference.md), and
[resource-management.md](resource-management.md). This doc is about *how to
decide where the boundaries go*.

---

## Mental model

- A **Task** is one registered async function + a config (retries, timeout,
  DLQ policy, shutdown policy, heartbeat). Workers dequeue, execute, and
  record status per Task — not per DAG.
- A **DAG** is a graph of Tasks wired with `.then()` (chain/fan-out) and
  `.merge()` (fan-in), built with `DAGNode`. Retries, timeouts, and shutdown
  policy are all evaluated **per node**. A DAG has no retry policy of its
  own — only its constituent Tasks do.
- Failure recovery — retry, DLQ resubmit, `on_shutdown: RESUBMIT` — always
  means **re-running the whole Task function from the top**, not resuming
  partway through. There is no partial-execution checkpoint inside a Task.

That last point is the source of almost every subtle bug in task design:
anything inside a Task function that shouldn't happen twice will happen
twice if the Task is retried, resubmitted, or dead-lettered-and-replayed.

---

## The core rule: isolate non-idempotent actions in their own Task

If a step in your pipeline has a real-world side effect that is unsafe or
wasteful to repeat — sending an email, charging a card, posting to a
webhook, writing a non-upsert record to an external system — put it in its
own Task, downstream of the computation that produces its inputs. Don't
inline it into a larger task that also does retryable/idempotent work.

**Why:** retries, DLQ resubmits, and `RESUBMIT`-on-shutdown all re-execute a
Task's function from scratch. If `send_confirmation_email()` sits inside the
same task as `compute_order_total()`, then a transient failure *after* the
email send but before the task returns will retry the whole function —
resending the email — even though the failure had nothing to do with the
email step.

```python
# Bad: one retry of a flaky downstream call resends the email too
@register_task(name="finalize_order", max_retries=3, retry_delay=10)
async def finalize_order(order_id: int, **kwargs) -> dict:
    total = compute_total(order_id)
    await send_confirmation_email(order_id)   # side effect, buried mid-function
    await write_to_ledger(total)               # this is the flaky call
    return {"total": total}

# Good: split at the side-effect boundary
@register_task(name="compute_order_total", max_retries=3, retry_delay=10)
async def compute_order_total(order_id: int, **kwargs) -> dict:
    return {"total": compute_total(order_id)}

@register_task(name="write_ledger_entry", max_retries=3, retry_delay=10)
async def write_ledger_entry(total: Annotated[float, FromParent("total")], **kwargs) -> dict:
    await write_to_ledger(total)   # idempotent (upsert) or safe to retry alone
    return {}

@register_task(name="send_confirmation_email", max_retries=0, dead_letter_policy=DeadLetterPolicy.SAVE)
async def send_confirmation_email(order_id: int, **kwargs) -> dict:
    await send_email(order_id)    # isolated: retried (if at all) independently, never as a side effect of something else's retry
    return {}
```

Wire them as a chain (or independent fan-out branches if order doesn't
matter) so a retry of the ledger write can never re-trigger the email:

```python
compute = DAGNode("compute_order_total", parameters={"order_id": order_id})
ledger = DAGNode("write_ledger_entry")
email = DAGNode("send_confirmation_email", parameters={"order_id": order_id})
compute.then(ledger, email)   # both run after compute; retrying one never re-runs the other
```

**Rule of thumb:** if you wouldn't be comfortable with a step firing twice
in production because of an unrelated retry, it needs its own Task.

### When a side effect can stay inline

You don't need to split out *every* external call — only ones where
repetition is unsafe or costly. A call that's naturally idempotent (HTTP PUT
to a fixed resource, an upsert keyed by a stable ID) is fine to leave inline
with `expected_exceptions` set so only the intended failures retry. Prefer
passing a stable idempotency key (`task.id` or `dag_run_id` — see below) into
external calls so *even a full task re-run* is safe, which removes the need
to split at all.

---

## Other reasons to split a Task, beyond idempotency

| Signal | Split because... |
| --- | --- |
| Different retry needs | A DB write might want `max_retries=5, EXPONENTIAL_JITTER`; a downstream Slack notification might want `max_retries=0` so it fails fast and goes to the DLQ instead of blocking the pipeline. One `max_retries`/`backoff_strategy` per Task — you can't have two policies in one function. |
| External call vs. local compute | Isolating slow/flaky external calls into their own Task lets you route them to a dedicated queue with its own `max_concurrent` and rate limit, without throttling the cheap computation steps that feed it. See [resource-management.md](resource-management.md). |
| Needs a heartbeat, most doesn't | Only long-running steps should set `max_heartbeat_interval` and call `task.heartbeat()`. Bundling a 5-second step with a 10-minute one forces the fast path to carry heartbeat plumbing it doesn't need. |
| Different DLQ handling | Steps worth manual inspection on failure (`dead_letter_policy=SAVE`) vs. steps that are fine to just drop (`NONE`) shouldn't share a Task. |
| Different `on_shutdown` needs | A non-idempotent step should almost never use `RESUBMIT` (see below); a cheap idempotent step often can. Splitting lets each carry the right policy. |
| Fan-out parallelism | If a step can run once per item (e.g. per record, per file), make it its own Task so `DynamicFanOut` / `-->>` can parallelize it, instead of looping over items inside one Task function. |

Conversely, **don't** split purely for code-organization reasons — two
`await`s that are both cheap, both idempotent, and always fail/retry
together are fine as one Task. Extra Tasks mean extra Redis round-trips,
extra queue hops, and extra places for a DAG wiring bug to hide. Split at
side-effect and policy boundaries, not at arbitrary function-length ones.

---

## Idempotency patterns for the Task you do write

- **Use a stable idempotency key.** `get_current_task().id` (the task's own
  ULID) or `dag_run_id` are stable across a retry of *the same* task
  attempt/run and can be passed to external APIs that support
  idempotency keys (Stripe, etc.) or used as a dedupe key in your own
  ledger table.
- **Prefer upserts over inserts** for any DB write inside a retryable Task.
- **Check-before-act** when the external system has no idempotency key
  support: look up whether the effect already happened before performing it.
- **Keep `expected_exceptions` narrow.** Only list exceptions that represent
  a genuinely transient condition. Anything else should fail immediately
  (`FAILED`, no retry) rather than silently repeating a side effect under a
  broad `except Exception`-equivalent.

---

## Config cheat sheet

Full field reference: [task-definition-reference.md](task-definition-reference.md).

### Retries

| Field | Use it to... |
| --- | --- |
| `max_retries` | Cap retry attempts. `0` for non-idempotent, fail-fast steps (email, notifications). |
| `retry_delay` + `backoff_strategy` | `EXPONENTIAL_JITTER` is the safe default for calls to shared downstream services (avoids thundering herd). `CONSTANT` for steps where wait time doesn't matter. Leave `retry_delay=None` only for cheap, purely-local retries. |
| `expected_exceptions` | Always set this explicitly for anything that retries. Unset means *no* exception retries — a common surprise. |

### Dead letter queue

- `dead_letter_policy=SAVE` for anything a human may need to inspect or
  manually resubmit after exhausting retries (payments, emails, anything
  with an external side effect).
- `dead_letter_policy=NONE` for steps that are safe to just drop on
  permanent failure (best-effort metrics, non-critical logging tasks).

### `on_shutdown` (SIGTERM behavior)

| Policy | Use for |
| --- | --- |
| `STOP` (default) | Non-idempotent or unclear-safety Tasks. Goes to `STALLED` for manual review rather than silently re-running. **Default choice for anything with a side effect.** |
| `RESUBMIT` | Only for Tasks that are fully idempotent (or side-effect-free) — the task is silently re-enqueued and will run again from the top. |
| `CONTINUE` | Only for Tasks that are cheap to let finish and where a shielded, uncancellable coroutine is acceptable. Blocks worker shutdown until the task completes — avoid for long-running work. |

### Heartbeats

Set `max_heartbeat_interval` (and call `task.heartbeat()` inside the loop)
only on Tasks that can run long enough to plausibly hang. Don't set it on
short tasks — it adds Cleaner bookkeeping for no benefit.

---

## Concurrency: which knob to use

See [resource-management.md](resource-management.md) for full detail. Quick
picture:

- **`WORKER_CONCURRENT_TASKS`** — total capacity of a worker process. Scale
  this for overall throughput.
- **Queue `max_concurrent`** — cap on one queue's in-flight tasks across a
  worker. Use this to protect a downstream dependency (e.g. `heavy-ml`
  queue capped at 2) independent of overall worker capacity.
- **Queue rate limiting** (`rate_numerator`/`rate_denominator`/`rate_period`)
  — enforced at submission time; use for hard external rate limits (e.g. a
  third-party API quota).
- **Task `max_concurrent`** — per-task-type cap within one worker process.
  Currently stored but **not enforced** — don't rely on it; use queue-level
  caps instead.

Put slow/external/rate-limited steps on their own queue (per the splitting
guidance above) so their concurrency and rate limits don't also throttle
unrelated fast tasks sharing a queue.

---

## Pre-merge checklist for a new Task or DAG

- [ ] Does any single Task function contain more than one side effect that
      shouldn't both repeat together on retry? If so, split it.
- [ ] Is `expected_exceptions` set explicitly (not left to catch nothing, or
      accidentally left broad)?
- [ ] Does a Task with a real side effect use `on_shutdown=STOP` (or `SAVE`
      DLQ policy) rather than `RESUBMIT`, unless it's genuinely idempotent?
- [ ] Are external calls passed a stable idempotency key (`task.id` /
      `dag_run_id`) where the downstream API supports one?
- [ ] Does a slow/external-dependency step live on its own queue, separate
      from fast/local steps, so it can carry its own concurrency/rate limits?
- [ ] For fan-in nodes (`DAGNode.merge`), do `FromParent` params use
      `many=True`? (Enforced at construction time via
      `FanInCardinalityError` — see [dag-composition.md](dag-composition.md).)
