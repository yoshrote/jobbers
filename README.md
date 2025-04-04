# Jobbers

A task/workflow framework similar to Celery/Airflow. The name is inspired by
the many wrestlers who anonymously do the work to make the stars look good.

## Key Principles

- asyncio native
- Useful info about the state of tasks and queues should be surfaced via metrics 
and traces without having to add on extra tooling so determining how to divide
tasks between workers and queues in order to keep load balanced.
- task concurrency controls (per task, per worker, per cluster; easy worker pooling)
- maybe dead letter queue for recovery

## Other niceties

- [ULID](https://github.com/ulid/spec) for task ids so that clients could
generate them in advance if needed. 

## Planned features

- Worker crash recovery
  - on shutdown (SIGTERM, SIGQUIT)
    - best effort to finish current tasks or update their state to know they died
  - on startup
    - check if we need to recover from something
  - on task submissions, create a record prior to sending out the job
    - task should include "last located at X step at HH:MM" that will be updated periodically to help debug if it is becomes stuck, crashed, or otherwise lost
    - maybe: the record should include an execution plan that include signals of how/how well the task is distributed
- task state
  - DAG
    - maps what to do (python function)
    - distribution strategy for 1 parent:N child tasks
      - round-robin, hash by key, etc
    - decide whether to inline multiple tasks in the same worker for 1:1 tasks or throw subtasks on the queue
  - current location within DAG
  - temp result location (in memory, local/remote file, etc)
- Long-running tasks can issue a heartbeat so that a user can differentiate between
a slow task and a frozen task
- on task read
  - (optional) update heartbeat
    - configuration needed to avoid thundering herd issues
    - TODO: how to handle if we want to track many operations downstream from another?
  - do the thing
  - (optional) update heartbeat again
    - depending on the expected duration of the task. a slow task would want a record, but a fast task may not
    - TODO: more high contention on updating any shared heartbeat
  - pass along the work to the next node in the DAG

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
- task timeout and retry policy
- rate limiting
  - per task + params
  - per queue
  - per task per queue
- batch fetching tasks off the queue to optimize large numbers of small tasks
- task chaining (DAG support)
- task distribution strategies across queues (random, hash by parameter, etc)
- enable good dashboards with useful metrics
- discover and handle when role -> queue mapping changes
