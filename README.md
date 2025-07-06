# Jobbers

A task/workflow framework similar to Celery/Airflow. The name is inspired by
the many wrestlers who anonymously do the work to make the stars look good.

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
- Basic retry policy (re-enqueue, but no backoff yet)
- The state of the queues and the last known state of tasks are stored for diagnostic and recovery purposes.

## Other niceties

- [ULID](https://github.com/ulid/spec) for task ids so that clients could generate them in advance if needed.

## Planned features

- Worker crash recovery
  - on shutdown (SIGTERM, SIGQUIT)
    - best effort to finish current tasks or update their state to know they died, otherwise more clever recovery code will be needed.
- task state
  - DAG
    - maps what to do (python function)
    - distribution strategy for 1 parent:N child tasks
      - round-robin, hash by key, etc
    - decide whether to inline multiple tasks in the same worker for 1:1 tasks or throw subtasks on the queue
  - current location within DAG (and summary/audit of where it's been)
  - temp result location (in memory, local/remote file, etc)
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
- implement task search and retry tools for debugging and recovery (generalize work from StateManager.clean)
- task concurrency controls (per task, per worker, per cluster; easy worker pooling)
  - determine how to divide tasks between workers and queues in order to keep load balanced

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
- task retry backoff policy
- rate limiting per task + params per pool (check on submission)
- batch fetching tasks off the queue to optimize large numbers of small tasks
- task chaining (DAG support)
  - task distribution strategies across queues (random, hash by parameter, etc)
- enable good dashboards with useful metrics
- discover and handle when role -> queue mapping changes
