"""Central SQLAlchemy DDL definitions for the Jobbers schema."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, MetaData, String, Table

metadata = MetaData()

roles = Table(
    "roles",
    metadata,
    Column("name", String, primary_key=True),
    Column("refresh_tag", String, nullable=False),
)

queues = Table(
    "queues",
    metadata,
    Column("name", String, primary_key=True),
    Column("max_concurrent", Integer, nullable=True),
    Column("rate_numerator", Integer, nullable=True),
    Column("rate_denominator", Integer, nullable=True),
    Column("rate_period", String, nullable=True),
)

role_queues = Table(
    "role_queues",
    metadata,
    Column(
        "role",
        String,
        ForeignKey("roles.name", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
    Column(
        "queue",
        String,
        ForeignKey("queues.name", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    ),
)

task_routing = Table(
    "task_routing",
    metadata,
    Column("task_name", String, primary_key=True),
    Column("task_version", Integer, primary_key=True),
    Column("strategy", String, nullable=False),
    Column("queues", String, nullable=False),  # JSON array of queue names
    Column("weights", String, nullable=True),  # JSON array of floats, NULL unless WEIGHTED
)

tasks = Table(
    "tasks",
    metadata,
    Column("id", String, primary_key=True),
    Column("name", String, nullable=False),
    Column("queue", String, nullable=False),
    Column("version", Integer, nullable=False, default=0),
    Column("parameters", String, nullable=False, default="{}"),   # JSON
    Column("results", String, nullable=False, default="{}"),      # JSON
    Column("errors", String, nullable=False, default="[]"),       # JSON
    Column("retry_attempt", Integer, nullable=False, default=0),
    Column("status", String, nullable=False),
    Column("submitted_at", String, nullable=True),                # ISO 8601
    Column("retried_at", String, nullable=True),
    Column("started_at", String, nullable=True),
    Column("heartbeat_at", String, nullable=True),
    Column("completed_at", String, nullable=True),
    Column("dag_callbacks", String, nullable=False, default="[]"),  # JSON
    Column("parent_ids", String, nullable=False, default="[]"),     # JSON
    Column("inject_parent_results", Boolean, nullable=False, default=False),
    Column("cron_id", String, nullable=True),
    Column("dag_run_id", String, nullable=True),
)

task_queue = Table(
    "task_queue",
    metadata,
    Column("queue", String, primary_key=True),
    Column("task_id", String, ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True),
    Column("submitted_at", String, nullable=False),  # ISO 8601; lexicographic order == time order
)

rate_limiter = Table(
    "rate_limiter",
    metadata,
    Column("queue", String, primary_key=True),
    Column("task_id", String, primary_key=True),
    Column("submitted_at", String, nullable=False),  # ISO 8601
)

dead_letter_queue = Table(
    "dead_letter_queue",
    metadata,
    Column("task_id", String, ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True),
    Column("queue", String, nullable=False),
    Column("name", String, nullable=False),
    Column("version", Integer, nullable=False),
    Column("failed_at", String, nullable=False),   # ISO 8601
)

scheduled_tasks = Table(
    "scheduled_tasks",
    metadata,
    Column("task_id", String, ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True),
    Column("queue", String, nullable=False),
    Column("run_at", String, nullable=False),       # ISO 8601
)

dlq_missing_data = Table(
    "dlq_missing_data",
    metadata,
    Column("task_id", String, primary_key=True),
    Column("recorded_at", String, nullable=False),  # ISO 8601
)

Index("idx_roles_refresh_tag", roles.c.refresh_tag)
Index("idx_role_queues_role_queue", role_queues.c.role, role_queues.c.queue)
Index("idx_tasks_queue_status", tasks.c.queue, tasks.c.status)
Index("idx_tasks_heartbeat", tasks.c.heartbeat_at)
Index("idx_task_queue_submitted_at", task_queue.c.submitted_at)
Index("idx_scheduled_tasks_run_at", scheduled_tasks.c.run_at)
Index("idx_dlq_failed_at", dead_letter_queue.c.failed_at)
