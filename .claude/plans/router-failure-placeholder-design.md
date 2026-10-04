# Router Failure Placeholder — Design Proposal

Status: **draft, pending review** — no implementation yet.

Audience: jobbers maintainers, and a future session picking this up cold.

## 1. Problem

A router failure is the only failure in jobbers that is **un-operable**. When
`_select_candidate` raises `RouterError` (`task_processor.py:443-502`), the exception
reaches `_handle_post_process_failure` (`task_processor.py:426-440`), which appends a
string to `task.errors`, increments `post_process_failures`, and re-saves the parent.
That is the entire outcome. There is:

- no task record representing the failed routing, so nothing in the UI, nothing in
  `/dead-letter-queue`, no `/dead-letter-queue/{id}/history`;
- no status — the parent stays `COMPLETED`, which is correct (it did its own work) but
  means the DAG run looks healthy;
- no way to retry the routing. `should_retry()` is a property of a *task*, and the
  routing attempt is not one;
- no way to retry it *after a fix* without re-running the parent task, which is the
  expensive and semantically wrong thing to do — the parent's results were fine.

Two live bugs compound this (both must be fixed for anything here to work):

- **`task_processor.py:408`** — the router loop lets `RouterError` escape
  `post_process` before the `if task.has_callbacks():` block, so one router's failure
  silently drops the parent's *unrelated* `SimpleCallback`/`FanInCallback`
  continuations.
- **`task_processor.py:504`** — `RouterCallback.error_callback` is never submitted on a
  router failure. The only reader of that field is `Task.generate_error_callbacks()`
  (`models/task.py:216-227`), reached only from `post_process_error` on the parent's
  `FAILED` path. The documented `R -.-> err` edge therefore fires on the wrong trigger
  entirely.

## 2. Proposal

On `RouterError`, create a **placeholder task** that stands for "re-run this router
against the parent's stored results". It is a real task in the DAG run, in `FAILED`
status, carrying the `RouterSpec` and `parent_ids=[parent.id]`. Resuming it re-executes
the router function; it never re-runs the parent and never re-triggers the parent's
siblings.

The assumption being encoded: **a router failure is usually the router's own bug**, and
a router is a pure function of results that are already persisted, so a code fix plus a
deploy is sufficient to make the same inputs route correctly. When the real fault was
upstream (the parent produced bad results), re-running the router will not help — but
the placeholder is still the right artifact, because the operator's remedy differs
while the record they need does not. See §6.1.

## 3. Why this fits the existing machinery

### 3.1 It converts an un-operable failure into an ordinary one

Everything in §1's bullet list is already solved for tasks. A `FAILED` placeholder
inherits `DeadLetterPolicy.SAVE`, the DLQ browse/history endpoints, DAG-run
cancellation, and the task detail page for free.

### 3.2 Pre-assigned candidate ULIDs make re-running idempotent

`RouterSpec.candidates` are `DAGTaskSpec`s whose ids are fixed at parse time. A re-run
router that picks the same candidate submits the **same task id** — an overwrite, not a
duplicate. If the fix makes it pick differently, that is a different id from the same
declared set. Either way the live diagram stays consistent and resuming twice is
harmless.

This is underwritten by the parse-time rule that a router has exactly one incoming edge
and that a candidate's only incoming edge is its router's (`mermaid_dag.py:531-558`):
one router owns one set of candidate ids, so there is never a question of which
placeholder is responsible for them.

### 3.3 `POST /dags/{id}/resume` already does the work

`FAILED` is in `TaskStatus.stuck_statuses()` (`models/task_status.py:34-46`), so a
`FAILED` placeholder inside a DAG run is picked up by `can_resume_dag_run`
(`state_manager.py:862`) and `resume_dag_run` (`state_manager.py:897`) with **no new
operator surface at all**. Resume reuses the stuck task's existing blob, sets
`SUBMITTED`, resets `retry_attempt`, reconciles the run's failed counter, and requeues.
When the placeholder then runs, it re-executes the router.

This is a better resume path than `POST /dead-letter-queue/resubmit`, because
`GET /dags/{id}/resume-check` (`task_routes.py:810`) tells the operator *up front*
whether the run is still resumable, instead of failing at resubmit time.

## 4. The retention question

> result retention is required to resume a failed DAG anyway (i think) so is it being a
> hard constraint a real change?

**Correct, and more strongly than "a hard constraint" implied — it is not a new
constraint at all.** `can_resume_dag_run` calls `get_tasks_bulk(run.task_ids)` and
returns `DAGResumeReason.TASK_HISTORY_INCOMPLETE` if **any** task blob in the run is
missing. That is strictly stronger than "the parent's results survive": resume already
requires the *whole run's* history. The placeholder inherits an existing operational
contract and adds nothing.

**So: do not snapshot the parent's results onto the placeholder.** That was the wrong
call in the earlier sketch. Snapshotting would duplicate data that must be retained
anyway for any resume to work, and would give a *weaker* guarantee than the existing
precheck — a self-contained blob, but no up-front signal about the rest of the run.

### 4.1 A pre-existing gap this surfaces

There is a real bug here, independent of routers, worth fixing on its own merits:
`clean_terminal_tasks` (`adapters/_shared.py:373-396`) is a **flat age sweep** on
`completed_at` with no DAG-run awareness. It deletes a `COMPLETED` parent's blob once it
is older than `completed_task_age` regardless of whether its run contains a stuck task
awaiting resume.

`stuck_statuses()`'s docstring says a run containing a stuck task is "left untouched
(fan-in tracking, sibling task records) so it can be manually resumed" — but that
protection applies to the *cleanup-on-completion sweep*, not to the age sweep. So today,
set `completed_task_age` shorter than the window in which an operator notices and
resumes, and the run becomes permanently unresumable with `TASK_HISTORY_INCOMPLETE`.

Options (not decided):

- make `clean_terminal_tasks` skip tasks whose `dag_run_id` names a run with a stuck
  task — correct, but adds a lookup per candidate blob to a full `scan_iter` sweep;
- document the `completed_task_age` vs. resume-window relationship in
  `docs/operations.md` and leave the sweep alone;
- surface run-resumability expiry as a metric so it is observable rather than silent.

The second is the cheap stopgap; the first is the actual fix. Either way this is a
prerequisite for *trusting* the placeholder, not for building it.

### 4.2 `fan_in_ttl` is the same class of constraint

`FanInCallback.fan_in_ttl` defaults to 86400 (`models/dag.py:215`). A placeholder
waiting on a bug-fix deploy for longer than that hits
`DAGResumeReason.FAN_IN_TRACKING_EXPIRED`. `refresh_dag_run_fan_in_ttl` exists and
`resume_dag_run` already calls it, so the ceiling is "one TTL window per resume
attempt", not absolute. Worth stating in the docs; no code change implied.

## 5. Design

### 5.1 The system task

Named in a reserved namespace, registered eagerly, never lazily.

**Naming.** `_LABEL_RE` (`mermaid_dag.py:186-188`) constrains task names to
`[a-zA-Z_][a-zA-Z0-9_]*` — **no dots**. `jobbers.rerun_router` would be unparseable as a
mermaid label and would break `dag_spec_to_mermaid` round-tripping. Proposed:
`jobbers__rerun_router` (double underscore). Grammar-safe, greppable, unambiguous.

Reserve the prefix: `register_task` (`registry.py:60`) rejects any *user* task whose
name starts with `jobbers__`, and the system registration path is the only thing allowed
to use it. This also keeps the namespace available for future system tasks.

**Registration.** A `register_system_tasks()` function called from two places:

1. once at startup, in whatever each runner shares before `init_state_manager()`;
2. at the end of the registry reset, so system tasks always exist. Eager
   re-registration rather than lazy — `validate_task` (`validation.py:12-16`) rejects
   anything not in the registry, and a lazily registered system task would make
   submission order significant.

**Decided:** rename `clear_registry()` (`registry.py:175-178`) to **`reset_registry()`**
and have it re-seed system tasks after clearing. "Reset" states the intent — back to the
baseline, which includes the system tasks — where "clear" would be a lie the moment it
re-seeds anything. This avoids the alternative (a separate `_system_task_map` that
`get_task_config`/`get_tasks` fall back to), which is a larger change for no behavioural
gain.

33 call sites, all in tests plus the `registry.py` definition. Mechanical rename.

### 5.2 What the placeholder carries

- `name="jobbers__rerun_router"`, `version=0`
- **`id` — the router node's own `RouterSpec.id`** (§5.8)
- `queue` — the parent's queue (§5.4)
- `parent_ids=[parent.id]`, `dag_run_id=parent.dag_run_id`
- `parameters` — the serialised `RouterSpec` (router name, version, parameters,
  candidate `DAGTaskSpec`s with their pre-assigned ids) and the failure mode (§5.6)
- `status` — `FAILED` under `HALT`, `COMPLETED` under `FALLBACK` (§5.7, §5.8); either way
  with the `RouterError` message in `task.errors`
- `dag_callbacks` — under `HALT`, the parent's `FanInCallback`s copied across by the
  promotion in §5.5, and nothing else. Under `FALLBACK`, none. The placeholder's handler
  submits the chosen candidate directly, exactly as `_handle_router` does inline today.

It is created **directly in a terminal status** rather than submitted and allowed to
run: the router has already failed inline, so queueing it for a guaranteed-to-fail first
execution would waste a round trip and muddle `retry_attempt`. Its handler therefore only
ever executes on a resume, and under `FALLBACK` never executes at all. This requires the
creation path to do what a normal submit would for run bookkeeping — add it to the DAG run
index and record the terminal outcome via `record_dag_run_task_terminal` — which
`resume_dag_run` then reconciles via `reconcile_dag_run_task_retry`.
**Needs care; see §7.1.**

### 5.3 The handler

```python
async def rerun_router(**params) -> dict:
    spec = RouterSpec.model_validate(params["router_spec"])
    parent = await state_manager.task_state.get_task(params["parent_id"])
    chosen = _select_candidate(spec, parent.results, parent, mode="simple")
    ...  # submit chosen, or complete as "declined" when None
```

Reuses `_select_candidate` verbatim so the retry path and the inline path cannot
diverge. If the re-run router raises again, the placeholder fails again — same state as
before, resumable again after the next fix. That is the desired loop.

### 5.4 Queue: the parent's

Per the ask. Consistent with `Task.queue` freezing at first submit, and with routers
deliberately having no queue of their own (`:queue` on a router label is a parse error).
Cost to accept: a router failure consumes parent-queue concurrency and is subject to
that queue's rate limit. Acceptable — it is one task per failure, and the alternative (a
dedicated system queue) would have to exist in every deployment and be a member of some
role, which the static backend makes awkward.

### 5.5 Fan-in: one mechanism, "the branch successor inherits the obligation"

The ask is "the placeholder should add itself to any fan-in of its parent to make sure
we account for this pause in processing". The right primitive **already exists**:
`delegate_fan_in(dag_run_id, fan_in_key, old_id, new_id)` (`protocols.py:281-287`)
atomically replaces `old_id` with `new_id` in both the tracking set and the permanent
members data.

**Correction to a premise worth recording:** an `-.->` error-callback child does **not**
currently promote itself into its parent's fan-in, and nothing in the codebase does this
for error callbacks. `generate_error_callbacks()` builds the child with
`parent_ids=[self.id]` and no fan-in involvement, and the *only* `delegate_fan_in` caller
is the fan-out collector-promotion path (`task_processor.py:600-618`). For a genuinely
`FAILED` parent that is correct — the parent's work did not happen, so the collector
*should* block and the run *should* need a resume. But it means promotion into a fan-in
has to be built deliberately for any case where a successor is a legitimate *substitute*
for the branch. That is both of ours:

| Policy | Successor that takes over the branch | Promoted into the parent's fan-ins |
| --- | --- | --- |
| `HALT` | the placeholder | yes |
| `FALLBACK` | the declared `-.->` node | yes |

So it is **one mechanism with two possible successors**, not two mechanisms. Note that
`_promote_collector_into_outer_fan_in` does it in two halves, and both are needed:
it copies the outer `FanInCallback`s onto the successor's `dag_callbacks` *and* calls
`delegate_fan_in`. Copying the callbacks is what lets the successor discharge the
obligation when it finishes; delegating is what stops the parent discharging it early.

Delegation is strictly better than add-then-remove:

- **no sizing window.** A `SADD` followed later by the parent's `fan_in_complete` `SREM`
  leaves the set transiently over-sized; a 1-for-1 swap never does.
- **cardinality is preserved**, so `validate_fan_in_cardinality`'s parse-time guarantee
  still holds. Adding a member at runtime would violate it.
- **precedent.** This is exactly the `propagate_fan_in` pattern: a nested fan-out arm
  swaps its own id for the grandcollector's so the outer fan-in waits for the
  grandcollector. "Swap the parent's id for the placeholder's so the outer fan-in waits
  for the routing to resolve" is the same shape.

So on `RouterError`, for each `FanInCallback` in `parent.dag_callbacks`:
`delegate_fan_in(dag_run_id, cb.fan_in_key, parent.id, placeholder.id)`.

The parent's own `generate_callbacks()` then calls `fan_in_complete(parent.id)`, which is
now a no-op against that set — precisely the intent. The collector waits for the
placeholder instead. When the placeholder's re-run succeeds it calls
`fan_in_complete(placeholder.id)` for each delegated key and the collector proceeds as it
would have.

**This makes the `task_processor.py:408` fix simpler, not harder.** Delegation must
happen *before* the `has_callbacks()` block, which is where the router loop already
sits — so the fix is to wrap `_handle_router` in `try/except RouterError` and keep the
ordering, not to reorder. Reordering was never the right fix.

Caveat: `delegate_fan_in` has no `stage_*` form, so it is a separate async call rather
than part of the pipeline that stages the parent's own callbacks. Within one worker the
ordering holds; a crash between delegation and the callbacks batch leaves the collector
waiting on a placeholder that exists, which is the safe direction.

### 5.6 Retryability keyed on *why*, not a flat setting

A router is a pure `def` over persisted results. Retrying it in 10 seconds with
exponential backoff fails **identically** — only a deploy changes the outcome. So
`max_retries=0` and the placeholder goes straight to `FAILED`; "retry" means an operator
resume.

One genuine exception: an **unregistered router** is transient. Mid-rolling-deploy, some
workers have the router and some do not, and a different worker picking the task up
succeeds. So split `RouterError`:

| Failure | Retryable in place? |
| --- | --- |
| `UnknownRouterError` (unregistered) | **Yes** — bounded retry with backoff |
| router raised | No — straight to `FAILED` |
| selection matched no candidate | No |
| selection ambiguous across candidates | No |
| returned a non-`RouteTo`/`str`/`None` value | No |

More useful than a configurable retry count, because the system can actually tell these
apart. Recorded in the placeholder's `parameters` so the handler and the UI can both see
it.

### 5.7 What becomes of `R -.-> err`

The placeholder takes over "the router's error handler", which frees the `-.->` edge to
mean what it is actually useful for: **the degraded path**.

**Decided: there is no policy enum. The behaviour is derived from whether a `-.->` edge
exists.** `RouterCallback.error_callback` is already `DAGTaskSpec | None`, so the policy
is exactly `FALLBACK if cb.error_callback is not None else HALT` — no new field, no new
mermaid syntax, and nothing extra to round-trip. A programmatically crafted
`RouterCallback` says which it wants by whether it passes `error_callback`.

| | `HALT` (no `-.->`) | `FALLBACK` (`-.->` present) |
| --- | --- | --- |
| Placeholder created | yes, `FAILED` | yes, `COMPLETED` (§5.8) |
| Placeholder promoted into parent's fan-ins | **yes** — it is the successor | **no** — it is only a record |
| Fallback node submitted | — | yes, and **it** is promoted into the fan-ins |
| Run stuck / resumable | yes | no |

**Decided: both paths create a placeholder.** The earlier draft had `FALLBACK` create
none; reversed. A placeholder is created only when a router *fails*, so the cost is
proportional to failures, not to DAG size — the happy path stores nothing extra (§5.8).
In exchange the state model is uniform: the router node's state always lives in exactly
one place, there is no second mechanism for "where does the degraded marker go", and the
frontend lookup does not need to special-case one policy.

What differs is only whether the placeholder *participates*. Under `HALT` it is the
branch's successor and inherits the parent's fan-in obligation. Under `FALLBACK` the
fallback node is the successor and gets promoted instead; the placeholder is inert — a
record that routing did not succeed, nothing waits on it.

Consequence of `FALLBACK`: no resume path for the routing itself. That is the correct
trade — the operator chose a degraded continuation over a halt, and getting both would
require the run to be simultaneously stuck and progressing.

**Expressiveness lost, deliberately:** "halt *and* notify" is no longer sayable, because
`-.->` now means "fall back" rather than "alert". Acceptable, because the placeholder is
itself a `FAILED` task — anything already watching for `FAILED` tasks or DLQ arrivals
notifies on it without needing a DAG edge. Revisit only if that turns out not to be true
in practice.

This also fixes the `None`-vs-`RouterError` conflation: `models/dag.py:270` and
`docs/mermaid-dag-spec.md` both say the error edge fires when the router "selects
nothing", but `_select_candidate` treats a `None` return as a deliberate decline
(`task_processor.py:468-472`), and in per-item mode `None` is *how you drop an item*. A
declined route must not create a placeholder or fire an error path. Only `RouterError`
does.

### 5.8 The placeholder *is* the router node

**Decided:** give the placeholder the router node's own pre-assigned
`RouterSpec.id` as its task id, rather than a fresh ULID.

This is what makes "show the placeholder's state as the state of the router node"
(the ask) fall out with almost no new plumbing, and it resolves the question of whether
the placeholder deserves a node in the emitted diagram: it does not need one, because it
*is* the router node.

What already exists:

- `RouterSpec.id` is pre-assigned at parse time and **remapped per run** by
  `RouterSpec._remap` (`models/dag.py:249-256`) through the same shared `id_map` as task
  specs, so it is unique per DAG run. No collision between concurrent runs of one DAG.
- The grammar already has the slot. `_ROUTER_NODE_RE` (`mermaid_dag.py:197`) matches
  quoted forms first *specifically* so a router label carrying a reserved `{status}`
  suffix lexes correctly — `R{"route_by_size{small}"}` — and the parser strips it
  (`tests/utils/test_mermaid_router.py:140`). The emitter (`_router_line`,
  `mermaid_dag.py:1130-1137`) has never written one. So the suffix is a reserved,
  round-trip-safe, currently-unused slot.
- `classDef router fill:#E6D7FF,stroke:#7A4FBF` already exists
  (`mermaid_dag.py:236`) alongside the per-status classes.

What it buys:

- **Re-creation is idempotent.** A second failure of the same router writes the same task
  id — an overwrite, not a second placeholder. Matches the candidate-ULID property in
  §3.2.
- **The frontend needs no new mapping.** `DagDetail.jsx:37-42` strips the backend's
  `classDef` lines and `:::class` suffixes and re-applies its own classes keyed by node
  id. Because the router node's id *is* the placeholder's task id, the existing
  id-to-status lookup colours the rhombus with no change to the lookup itself.
- The run's task index contains the router node, which is also what lets
  `can_resume_dag_run` see it as stuck (§3.3).

#### No placeholder on the happy path

This is what keeps the storage concern ("more data to store/load per DAG") bounded: a
placeholder exists **only when a router fails**. A router that resolves normally submits
its candidate inline and writes nothing extra, so a healthy DAG run stores exactly what it
stores today. The cost scales with failures, not with the number of router nodes.

The corollary is a correction to an earlier draft of this table: a *successful* router
must **not** be coloured green, because there is no task record carrying its id to drive
that. It stays `router` purple — which is right anyway. Purple means "this is a decision
point"; the thing that went green is the candidate it picked, and that node is already on
the diagram.

#### Colouring

**Decided:** add a dedicated `classDef degraded` rather than overloading `stalled`'s
amber, so the colour for decision-node outcomes can move later without touching task
statuses.

| Router node state | Placeholder | Class |
| --- | --- | --- |
| resolved, candidate submitted | none | `router` purple (unchanged) |
| declined (`None`) | none | `router` purple (unchanged) |
| `HALT` — routing failed, run stuck | `FAILED` | `failed` red |
| `FALLBACK` — degraded path taken | `COMPLETED` + non-empty `errors` | **`degraded` amber** |

Both outcome rows are derivable from the placeholder alone, with no new fields: `FAILED`
means halted, `COMPLETED`-with-errors means fell back. The `errors` list is already
populated this way for post-process failures, so this reuses an existing convention.

`degraded` needs adding in **two** places: the backend `classDef` block
(`mermaid_dag.py:230-236`) and the frontend's own palette, since `DagDetail.jsx:37-42`
strips the backend's `classDef` lines and re-applies its own. Starting it at `stalled`'s
`#FFD580` is safe — shape already distinguishes a degraded rhombus from a stalled
rectangle — and the point of the separate class is that it can diverge on a whim.

#### Why `COMPLETED` for the `FALLBACK` placeholder

Forced by the status taxonomy, not chosen:
`terminal_statuses() - stuck_statuses() == {COMPLETED}`
(`models/task_status.py:30-46`). Every other terminal status — `FAILED`, `CANCELLED`,
`STALLED`, `DROPPED` — is in `stuck_statuses()`, so any of them would make the run look
stuck, light up `GET /dags/{id}/resume-check`, and get the placeholder re-queued by
`resume_dag_run` even though the fallback already ran. `COMPLETED` with a populated
`errors` list is the only terminal state that records the failure without claiming the run
needs intervention.

A new `TaskStatus.DEGRADED` would be more honest, but the blast radius is large —
`terminal_statuses`, `stuck_statuses`, the DAG-run outcome counters, the SQL status enum
and migration, `DagStatusBadge`, the DLQ. Not worth it for one caller; worth revisiting if
a second need appears.

### 5.9 Creating a terminal task outside the submit path

`save_task` + `add_to_dlq` are the right storage primitives but are **not sufficient**,
and the gap is load-bearing rather than cosmetic.

**DAG-run membership is established at enqueue time, not at save time.**
`get_dag_run` derives `task_ids` from `SUNION(DAG_RUN_PENDING, DAG_RUN_CLOSED)`
(`adapters/redis/task_state.py:221-236`), and membership of those sets originates in the
`SADD` inside `enqueue`/`submit_task` (`adapters/_shared.py:715-721`) — `save_task` only
writes the blob and the type index. A placeholder created with `save_task` alone would be
**invisible to `get_dag_run`**, therefore invisible to `can_resume_dag_run`, therefore
**not resumable** — and also absent from `GET /dags/{id}` and from the diagram's node
list. That would quietly break §3.3, which is the keystone of this whole design.

So the placeholder must be put into the run's sets explicitly, and which set is the end
state differs by policy — matching what each kind of real task looks like:

| | End state in the run's sets | Why |
| --- | --- | --- |
| `HALT` (`FAILED`) | stays in `PENDING` | `finalize_dag_run_task` deliberately never closes a stuck task, so this is exactly what a real `FAILED` DAG task looks like |
| `FALLBACK` (`COMPLETED`) | ends in `CLOSED` | matches a real completed task |

**Ordering constraint (`FALLBACK` only).** `close_dag_run_task_and_sweep` marks the run
complete and sweeps when `PENDING` reaches zero. If the placeholder is closed before the
fallback node has been submitted into `PENDING`, the count can transiently hit zero and
the run gets marked complete and swept while the degraded path is still being dispatched.
**Submit the fallback node first, then write and close the placeholder.** Same class of
constraint as delegate-before-complete in §5.5.

**Decided: this justifies a helper.** Something like
`StateManager.record_terminal_task(task, *, outcome)` owning: blob save, the
`PENDING`/`CLOSED` membership write, `record_dag_run_task_terminal` with the right
`DagRunOutcome`, and no DLQ write (§6.2). Open-coding this at the call site would mean
duplicating `finalize_dag_run_task`'s stuck-vs-non-stuck reasoning, which the comment at
`state_manager.py:1315-1344` exists specifically to centralise.

Note `finalize_dag_run_task` itself cannot simply be reused as-is: it assumes the task was
previously submitted and is therefore already in `PENDING`. The helper needs to establish
that membership first, or the two steps need to be folded together.

Which `DagRunOutcome` the `FALLBACK` placeholder reports is settled in §5.10: neither of
the two existing values, but a new `"degraded"`.

### 5.10 `DagRunStatus.DEGRADED`

**Decided:** a run that took a `FALLBACK` path reports a new `DagRunOutcome` of
`"degraded"` against a new `degraded` counter, and surfaces as a new
`DagRunStatus.DEGRADED`. A fallback must **not** increment `failed`.

The reason to keep it out of `failed`: a fallback is not a task failure, and conflating
them would poison the one aggregate operators filter on. `GET /dags` listing a degraded run
as `partial_failure` means "some task in here died" no longer means that, and the
`failed_count` on the run stops being a count of failures. Separating them keeps both
signals honest — and makes "which runs are quietly running on their degraded path" a
first-class question rather than something you reconstruct from task-level `errors`.

Note this is a **run** status, not a task status. The `FALLBACK` placeholder task stays
`COMPLETED` for the reasons in §5.8 (`terminal_statuses() - stuck_statuses() ==
{COMPLETED}`); what changes is only the outcome it *reports* to the run's counters. The
`TaskStatus.DEGRADED` dismissed earlier in §5.8 is a different and much larger change; this
one does not touch `terminal_statuses`, `stuck_statuses`, or any per-task logic.

#### Precedence

`failed` > `degraded` > `complete` > `running`. A real task failure still dominates: a run
that both fell back and lost a task reads `partial_failure`/`failed`, not `degraded`.
`degraded` is reachable only when `failed == 0 and degraded > 0`.

Like `partial_failure`, `degraded` is not inherently terminal — it reflects outcomes
recorded so far and can appear while sibling tasks are still running. That is consistent
with the existing model rather than a new wrinkle.

#### The trap: `mark_dag_run_complete` would clobber it

`_MARK_DAG_RUN_COMPLETE_SCRIPT` (`adapters/redis/task_state.py:47-53`) currently sets
`status='complete'` **iff `failed == 0`**. Left alone, the last task's close would overwrite
`degraded` with `complete` and the signal would vanish precisely when the run finished —
the moment it matters most. It has to become: `complete` iff `failed == 0 and degraded ==
0`, else `degraded` when `degraded > 0`.

This is the one change that is easy to miss, because everything else works without it right
up until the end of the run.

#### `reconcile_dag_run_task_retry` must recompute but not decrement

`_RECONCILE_DAG_RUN_TASK_RETRY_SCRIPT` decrements `failed` when stuck tasks are resumed. It
must include the `degraded` term in its status recompute, but must **not** decrement
`degraded`: resuming some other stuck task does not un-take a fallback that already
happened. A run that fell back and is later resumed is still a degraded run.

#### Blast radius, concretely

Smaller than it looks, because the status is stored as a free-form string:

| Change | Notes |
| --- | --- |
| `DagRunStatus.DEGRADED` + `DagRunOutcome` gains `"degraded"` | `models/dag.py:744-761` |
| Recompute + complete-gate + reconcile scripts | 3 scripts × 3 backends (`redis`, `redis_json` duplicate the Lua; `sql` has its own) |
| `degraded_count` column + migration | `dag_runs` in `migrations/schema.py:122-135` |
| `DagStatusBadge.jsx` | enumerates the run-status values; needs one more entry |
| **No** `openapi.json` change | the `DagRunStatus` enum is not spelled out there (zero occurrences of `partial_failure`) |
| **No** `can_resume_dag_run` change | it keys off task `stuck_statuses()`, not run status |
| **No** SQL enum migration for the status itself | `dag_runs.status` is a plain `String` with a `server_default`, not a constrained type |

## 6. Decisions

### 6.1 One policy, not two ("router's fault" vs "parent's fault")

The original sketch proposed configurable policies for "re-run the router" vs. "don't
continue". These collapse, and that is a feature: the system cannot know whose bug it
was, but it does not have to decide at failure time. The placeholder in `FAILED` is the
right artifact for both, and the remedy is chosen by the human at resume time, when they
actually know:

| Actual fault | Remedy |
| --- | --- |
| Router bug | fix, deploy, `POST /dags/{id}/resume` |
| Parent produced bad results | re-run the parent, or the whole DAG run; abandon the placeholder |
| Not worth continuing | cancel the DAG run |

"Halt and don't continue" is therefore not a second policy — it is what the placeholder
already does. It is a halt *with a handle on it*, which strictly dominates a halt
without one. The only genuinely orthogonal axis is §5.7's `HALT`/`FALLBACK`.

### 6.2 The `FALLBACK` placeholder does not go to the DLQ

**Decided: no.** The point of the degraded path is to let mostly-normal processing
continue. The DLQ is the waiting room for work that has *stopped* and needs operator
action to restart — if that is what you want, you use `HALT`, which is exactly the
placeholder that does land in the DLQ. Routing a `COMPLETED` task into a dead-letter queue
would also invert what the DLQ means to anyone reading it.

Consequence to accept: a `FALLBACK` router bug is not visible in the DLQ at all. §6.3's
metrics are how it becomes visible, with the amber node and the placeholder's `errors` as the
per-run record.

### 6.3 Router failures and fallbacks get their own metrics

**Decided:** do not reuse `post_process_failures`. A fallback is not a post-process
failure in that counter's current sense — the run continued — and a halt is a
*router* failure specifically, which is worth distinguishing from the generic
post-process bucket it currently shares with store errors and fan-in misconfiguration.

Two new counters alongside the existing `router_decisions`:

| Metric | Tags | Fires when |
| --- | --- | --- |
| `router_failures` | `router`, `reason` (the §5.6 `RouterError` kind) | a router raises, is unregistered, or resolves to zero/several candidates |
| `router_fallbacks` | `router` | a `RouterError` is absorbed by a `-.->` degraded path |

`router_failures` fires for both policies; `router_fallbacks` is the subset that continued.
`halted = router_failures - router_fallbacks` falls out without a third counter. The
`reason` tag is what makes an unregistered-router spike during a rolling deploy (§5.6)
distinguishable from a genuine logic bug, which is the main thing an operator wants to know
at 3am.

### 6.4 Simple mode only

A `-->>` router failure fails the whole fan-out by design ("partial dispatch would be
worse than none"). A placeholder there would have to re-run the router over *every* item
and re-dispatch the arms — that is re-doing the fan-out, not re-doing the routing — and
the collector's fan-in may already be sized from arms spawned before the failing item.
Out of scope. `-->>` keeps its current all-or-nothing behaviour and
`_handle_declarative_fanout` is untouched.

## 7. Open questions

Nothing here blocks starting implementation.

1. **§5.9** — does the helper fold the `PENDING` membership write into
   `finalize_dag_run_task`, or establish membership first and then call it? The atomic
   backends would prefer one pipeline; the saga path does not care. Decide while writing
   it.
2. **§5.10** — should `GET /dags` gain a `degraded` filter value, or is the badge enough?
   Depends on whether anyone wants to alert on it; the metric in §6.3 may cover that need
   already.
3. **§4.1** — `clean_terminal_tasks` DAG-run awareness: skip-if-run-has-stuck-task, or
   document the `completed_task_age` relationship and move on? Independent of this feature
   and can be decided separately.

## 8. Implementation order

1. **Prerequisite:** fix `task_processor.py:408` — wrap `_handle_router` in
   `try/except RouterError` so the parent's unrelated callbacks still fire. Testable on
   its own, valuable on its own.
2. Split `RouterError` into the subtypes in §5.6. No behaviour change yet.
3. Rename `clear_registry` to `reset_registry` (§5.1). Pure mechanical rename across 33
   call sites, no re-seeding yet — lands cleanly on its own.
4. Reserved `jobbers__` namespace + `register_system_tasks()`, re-seeded from
   `reset_registry` (§5.1), with the `register_task` rejection for user tasks.
5. `StateManager.record_terminal_task` helper (§5.9): blob save + `PENDING`/`CLOSED`
   membership + `record_dag_run_task_terminal`. Independently testable against the
   `task_adapter` fixture, and the piece most likely to have surprises.
6. `jobbers__rerun_router` task, created in `FAILED` on `RouterError` under the router
   node's own id (§5.2, §5.8), via step 5's helper. This is the whole `HALT` path, and it
   is already useful on its own: resume works via the existing endpoints.
7. Fan-in promotion for the placeholder (§5.5) — copy the parent's `FanInCallback`s onto
   it *and* `delegate_fan_in`.
8. `DagRunStatus.DEGRADED` + the `degraded` counter (§5.10): enum, the three recompute
   scripts across three backends, the `mark_dag_run_complete` gate, the migration, and the
   badge. **Independent of the router work** — it is a self-contained run-status addition
   and can be built and tested on its own, in parallel with steps 5-7.
9. The `FALLBACK` path (§5.7): derive it from `error_callback is not None`, submit the
   fallback node **first** (§5.9's ordering constraint), promote *it* instead of the
   placeholder via step 7's mechanism, then write and close the placeholder `COMPLETED`
   reporting the `"degraded"` outcome from step 8. Includes the `None`-vs-`RouterError` doc
   correction.
10. `router_failures` / `router_fallbacks` counters (§6.3). Trivial once 6 and 9 exist.
11. `classDef degraded` + router node status emission in `_router_line`, and the matching
    frontend palette entry (§5.8). Purely presentational — can land last or in parallel.
12. Separately, on its own merits: `clean_terminal_tasks` DAG-run awareness (§4.1).

Per `CLAUDE.md`'s testing rules, steps 5-9 touch submit ordering, DAG-run accounting and
fan-in sizing — none of which `DummyTaskAdapter` replicates. Each needs at least one
`state_manager_real_ta` or `task_adapter` test, not only orchestration coverage. Two cases
are real-backend-only by nature: step 9's premature-sweep hazard (§5.9), and step 8's
`mark_dag_run_complete` gate (§5.10), which only misbehaves on a run's *last* close and so
cannot be caught by a single-task test.
