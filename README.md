# Jobbers

A task execution framework similar to Celery, but with a focus on providing
support to track and handle the recovery of long running tasks that stall or
fail.

The name is inspired by the many pro wrestlers who anonymously do the work to
make the stars look good. A future feature would be to implement an Airflow
Executor for this.

This framework is intended to allow tasks to make use of asyncio and aims to
help manage and recover from failures or hang ups that can occur in long running
tasks.

Long running jobs may hang indefinitely for any number of reasons given the
arbitrary code that may be running. Detecting workers in this state and having
tools to provide options to recover or kill the tasks are useful to handle these kinds of operational issues.

## OpenTelemetry Metrics

aggregated by task, version, and queue

| metric | type | description |
| ------ | ---- | ----------- |
|  tasks_retried      | COUNT| one per retry event (expected exception or timeout) |
|  tasks_dead_lettered | COUNT | tasks moved to the dead letter queue |

aggregated by task, queue, and status

| metric | type | description |
| ------ | ---- | ----------- |
|  tasks_processed    | COUNT| number of tasks |
|  execution_time     | HIST | time from task start to task finish (ms) |
|  end_to_end_latency | HIST | time from enqueue to task finish (ms) |

aggregated by task, queue, and role

| metric | type | description |
| ------ | ---- | ----------- |
|  time_in_queue   |HIST  | time from enqueue to task start |
|  tasks_selected  |COUNT | number of tasks |

### Useful Insights via OTEL

`time_in_queue`

- judge if more or less workers should be deployed
- judge if tasks may be merged into a single queue or should be separated for scaling

`tasks_processed[status=dropped]`

- workers are losing or dropping tasks. possibly an old worker receiving new tasks or a bad client sending garbage.

### Common Operational Questions/Issues TODO

Task Health and Management

- (API) status state
- Detect, halt, and recover frozen tasks (e.g. stalled import/export tasks)
  - Heartbeats provide a way to detect if a task is frozen.
  - Including checkpoint info could allow a recovering task to skip redundant work.
- Detect and recover jobs whose worker were killed (e.g. deployments or aggressive restarts)
  - Either a new worker should spin up to take over for the dead one or the jobs need to be re-enqueued to be distributed among the remaining workers.
- which tasks are currently being run, where, and for how long thus far?
Queue Health and Management
- (redis q) how long are the queues? ZCARD queue
- (otel) how fast are tasks submitted?
Worker Health:
- (otel) worker uptime
- (API) which tasks are running/ran on which worker?

## How it works

Jobbers is split into two applications and a cleanup utility.

`manager` is a web application to submit tasks, request results, and get the
current state of the tasks and queues.
The manager acts similar to the RMQ exchange in a Celery/Airflow system by
determining which Redis queue should be used for a particular job.

Multiple `managers` can be deployed to scale the reads/writes to Redis, but if Redis
becomes the bottleneck then you could shard your workflows across multiple manager/redis clusters
based on the queues, a parameter (such as a user id) or whatever else to segment the overall
system across multiple Redis instances.

`worker` is a task consumer to pull tasks from queues and run the actual task code.
Each worker is independent of the others and so these can be scaled arbitrarily so
long as the underlying Redis database can handle all of the connections.

`cleaner` clears task state and queues of jobs that meet search criteria

- task type
- time of submission
- queue name

## Key Principles

- asyncio native
- Useful info about the state of tasks and queues should be surfaced via metrics and traces without having to add on extra tooling.

### Task failure and recovery

Given that we are tracking the state of each task while they exist in a queue-like structure, implementing some dead-letter resubmission feature should be easy enough. Depending on the desired policy (at-most-once vs at-least-once) and the task state we can determine which tasks are safe to retry from failure and which should stay failed.

A tool that could do this would run at some cadence to monitor for tasks in a retryable state (configured for whatever is appropriate: specific failures, time outs, infra failure, etc) and resubmit them. APIs to surface those tasks would be useful for observation purposes. Submitting the mass retry may be better handled with a dedicated API rather than forcing a user to make a trip to get all of the tasks and somewhere between 1 and N calls to resubmit those tasks.

## Features

- Worker load management
  - Memory leak protection via max tasks (env: WORKER_TTL)
    The number of worker processes and restarting them is left to other tools.
  - worker concurrency (env: WORKER_CONCURRENT_TASKS)
  - capacity limits per worker per queue (TBD config)
- Configurable retry backoff policy (constant, linear, exponential, exponential with jitter)
- Dead letter queue with per-task failure history and bulk resubmission API
- The state of the queues and the last known state of tasks are stored for diagnostic and recovery purposes.
- Worker crash recovery
  - on SIGTERM, a worker will handle currently running tasks according to their shutdown policy
  - policies: finish, re-enqueue, or fail.

## Other niceties

- [ULID](https://github.com/ulid/spec) for task ids so that clients could generate them in advance if needed.

## Planned features

- system/smoke tests
- Long-running tasks can issue a heartbeat so that a user can differentiate between
a slow task and a frozen task
- on task read
  1. (optional) update heartbeat
     - configuration needed to avoid thundering herd issues
     - TODO: how to handle if we want to track many operations downstream from another?
  2. do the thing
  3. (optional) update heartbeat again
     - depending on the expected duration of the task. a slow task would want a record, but a fast task may not
     - TODO: more high contention on updating any shared heartbeat
  4. pass along the work to the next node in the DAG
- Validate the APIs against registered queues to avoid garbage input
- implement task search and retry tools for debugging and recovery (generalize work from StateManager.clean)
- Wrap task functions can be called via `foo.delay()` like Celery does to submit jobs
- Authentication for the API
- task DAG
  - maps what to do (python function)
  - distribution strategy for 1 parent:N child tasks
    - round-robin, hash by key, etc
  - decide whether to inline multiple tasks in the same worker for 1:1 tasks or throw subtasks on the queue
  - current location within DAG (and summary/audit of where it's been)

Tech stack

- Python3
  - <https://github.com/aiortc/aioquic> for http server support
- Redis for task state management
- Redis for task queue implementation (to be pluggable with Kafka)
- Open Telemetry for Admin
  - configure trace and metric granularity per task type to aggregate small tasks for efficiently
  - <https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html>
  - uptrace.dev as otel viewer/collector (docker-compose to orchestrate?)
- FastAPI for task submission, task status, and Admin API/UX

Side quests:

- We can do code generation from an input file
- run/port airflow workflows and tasks

Top Problems:

- example task
- heartbeat reporting hook for example task
- rate limiting per task + params per pool (check on submission)
- batch fetching tasks off the queue to optimize large numbers of small tasks
- task chaining (DAG support)
  - task distribution strategies across queues (random, hash by parameter, etc)
- enable good dashboards with useful metrics
  - total queue length over time
  - time-in-queue aggregates per queue
  - which workers are/were running which tasks (diagnostics and failure recovery)
  - aggregate task execution time (possibly per task+queue in case the queue assignment is significant)
  - task completion rates (per task or per task per queue)
- discover and handle when role -> queue mapping changes

## Retry and Dead Letter Queue Configuration

### TaskConfig retry fields

| field | type | default | description |
| ----- | ---- | ------- | ----------- |
| `max_retries` | `int` | `3` | Maximum number of retry attempts before the task is failed permanently |
| `retry_delay` | `int \| None` | `None` | Base delay in seconds between retries. When `None`, retries are immediate (no backoff). |
| `backoff_strategy` | `BackoffStrategy` | `exponential` | How the delay grows with each attempt (see below) |
| `max_retry_delay` | `int` | `3600` | Upper bound on the computed delay in seconds (1 hour) |
| `dead_letter_policy` | `DeadLetterPolicy` | `none` | Whether to persist permanently failed tasks for later inspection |

### BackoffStrategy

| value | behaviour |
| ----- | --------- |
| `constant` | Fixed delay of `retry_delay` seconds each attempt |
| `linear` | `retry_delay * attempt` seconds |
| `exponential` | `retry_delay * 2^attempt` seconds |
| `exponential_jitter` | Uniform random value in `[0, retry_delay * 2^attempt]` to spread out thundering-herd retries |

All strategies are capped at `max_retry_delay`.

### DeadLetterPolicy

| value | behaviour |
| ----- | --------- |
| `none` | Failed tasks stay in FAILED status in Redis only |
| `save` | Failed tasks are also written to the SQLite dead letter queue for later inspection and resubmission. Each failure event is appended to the task's audit history. |

### Example

```python
from jobbers.registry import register_task
from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy

@register_task(
    name="my_task",
    version=1,
    max_retries=5,
    retry_delay=10,                        # 10s base delay
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    max_retry_delay=300,                   # cap at 5 minutes
    dead_letter_policy=DeadLetterPolicy.SAVE,
)
async def my_task(**kwargs):
    ...
```

### Dead Letter Queue API

| method | path | description |
| ------ | ---- | ----------- |
| `GET`  | `/dead-letter-queue` | Search DLQ entries; filter by `queue`, `task_name`, `task_version`, `limit` |
| `GET`  | `/dead-letter-queue/{task_id}/history` | Full chronological failure history for a task |
| `POST` | `/dead-letter-queue/resubmit` | Bulk resubmit DLQ tasks by explicit IDs or filter criteria |
| `GET`  | `/scheduled-tasks` | List tasks currently waiting for a delayed retry |

#### `POST /dead-letter-queue/resubmit` request body

| field | type | default | description |
| ----- | ---- | ------- | ----------- |
| `task_ids` | `list[str] \| null` | `null` | Explicit list of task IDs to resubmit |
| `queue` | `str \| null` | `null` | Filter: resubmit tasks from this queue |
| `task_name` | `str \| null` | `null` | Filter: resubmit tasks with this name |
| `task_version` | `int \| null` | `null` | Filter: resubmit tasks with this version |
| `reset_retry_count` | `bool` | `true` | Reset `retry_attempt` to 0 before resubmitting |
| `limit` | `int` | `100` | Maximum tasks to resubmit when using filter mode (max 1000) |

Provide either `task_ids` **or** at least one filter field (`queue`, `task_name`, `task_version`).

## Task Heartbeat Configuration

The heartbeat function allows you to monitor the health and progress of long-running tasks. Here's how to configure and use it:

### Basic Setup

```python
import datetime as dt
from jobbers.registry import register_task

# Create a task
@register_task(
  name="Long Running Job", 
  version=1, 
  max_heartbeat_interval=dt.timedelta(minutes=5)
)
def long_running_task(task):
  for chunk in get_list_of_things():
    do_the_thing(chunk)
    task.heartbeat()
```

If the task does not update its heartbeat within 5 minutes it may be detected
as having stalled and we can detect that and react.
