"""Central SQLAlchemy DDL definitions for the Jobbers schema."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, MetaData, String, Table, Text

metadata = MetaData()

# ---------------------------------------------------------------------------
# Routing tables (feature: "routing")
# ---------------------------------------------------------------------------

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

Index("idx_roles_refresh_tag", roles.c.refresh_tag)
Index("idx_role_queues_role_queue", role_queues.c.role, role_queues.c.queue)

# ---------------------------------------------------------------------------
# Task state tables (feature: "task_state")
# ---------------------------------------------------------------------------

tasks = Table(
    "tasks",
    metadata,
    Column("id", String(26), primary_key=True),
    Column("name", String, nullable=False),
    Column("queue", String, nullable=False),
    Column("version", Integer, nullable=False),
    Column("parameters", Text, nullable=False, default="{}"),
    Column("results", Text, nullable=False, default="{}"),
    Column("errors", Text, nullable=False, default="[]"),
    Column("status", String, nullable=False),
    Column("retry_attempt", Integer, nullable=False, default=0),
    Column("submitted_at", DateTime(timezone=True), nullable=True),
    Column("retried_at", DateTime(timezone=True), nullable=True),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("heartbeat_at", DateTime(timezone=True), nullable=True),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("parent_ids", Text, nullable=False, default="[]"),
    Column("inject_parent_results", Boolean, nullable=False, default=False),
    Column("cron_id", String(26), nullable=True),
    Column("dag_run_id", String(26), nullable=True),
    Column("dag_callbacks", Text, nullable=False, default="[]"),
)

Index("idx_tasks_queue_submitted", tasks.c.queue, tasks.c.submitted_at)
Index("idx_tasks_queue_status", tasks.c.queue, tasks.c.status)
Index("idx_tasks_queue_status_submitted", tasks.c.queue, tasks.c.status, tasks.c.submitted_at)
Index("idx_tasks_queue_name_status", tasks.c.queue, tasks.c.name, tasks.c.status)
Index("idx_tasks_name_status", tasks.c.name, tasks.c.status)
Index("idx_tasks_dag_run_id", tasks.c.dag_run_id)
Index("idx_tasks_queue_heartbeat", tasks.c.queue, tasks.c.heartbeat_at)
Index("idx_tasks_status_completed", tasks.c.status, tasks.c.completed_at)

task_queue = Table(
    "task_queue",
    metadata,
    Column(
        "task_id",
        String(26),
        ForeignKey("tasks.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("queue", String, nullable=False),
    Column("submitted_at", DateTime(timezone=True), nullable=False),
)

Index("idx_task_queue_queue_submitted", task_queue.c.queue, task_queue.c.submitted_at)

task_fan_in = Table(
    "task_fan_in",
    metadata,
    Column("fan_in_key", String, nullable=False, primary_key=True),
    Column("task_id", String(26), nullable=False, primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed", Boolean, nullable=False, server_default="0"),
)

Index("idx_task_fan_in_created", task_fan_in.c.created_at)

dag_runs = Table(
    "dag_runs",
    metadata,
    Column("dag_run_id", String(26), primary_key=True),
    Column("submitted_at", DateTime(timezone=True), nullable=False),
)

Index("idx_dag_runs_submitted", dag_runs.c.submitted_at)

# ---------------------------------------------------------------------------
# Dead-letter queue table (feature: "dead_letter")
# ---------------------------------------------------------------------------

dead_letter_queue = Table(
    "dead_letter_queue",
    metadata,
    Column("id", String(26), primary_key=True),
    Column("queue", String, nullable=False),
    Column("name", String, nullable=False),
    Column("version", Integer, nullable=False),
    Column("failed_at", DateTime(timezone=True), nullable=False),
    Column("task_data", Text, nullable=False),  # full serialised Task as JSON
)

Index("idx_dlq_failed_at", dead_letter_queue.c.failed_at)
Index("idx_dlq_queue", dead_letter_queue.c.queue)
Index("idx_dlq_name", dead_letter_queue.c.name)
Index("idx_dlq_queue_name", dead_letter_queue.c.queue, dead_letter_queue.c.name)

# ---------------------------------------------------------------------------
# Task scheduler table (feature: "task_schedule")
# ---------------------------------------------------------------------------

task_schedule = Table(
    "task_schedule",
    metadata,
    Column("task_id", String(26), primary_key=True),
    Column("queue", String, nullable=False),
    Column("task_data", Text, nullable=False),  # Task snapshot as JSON; self-contained
    Column("run_at", DateTime(timezone=True), nullable=False),
)

Index("idx_task_schedule_queue_run_at", task_schedule.c.queue, task_schedule.c.run_at)
Index("idx_task_schedule_run_at", task_schedule.c.run_at)

# ---------------------------------------------------------------------------
# Cron DAG scheduler tables (feature: "cron_dag")
# ---------------------------------------------------------------------------

cron_dag_entries = Table(
    "cron_dag_entries",
    metadata,
    Column("id", String(26), primary_key=True),
    Column("name", String, nullable=False),
    Column("cron_expr", String, nullable=False),
    Column("dag_spec", Text, nullable=False),  # CronDAGEntry.dag_spec as JSON
    Column("enabled", Boolean, nullable=False),
    Column("concurrency_policy", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("next_run_at", DateTime(timezone=True), nullable=True),  # NULL = acquired by scheduler
)

Index("idx_cron_dag_entries_next_run_at", cron_dag_entries.c.next_run_at)

cron_dag_active_runs = Table(
    "cron_dag_active_runs",
    metadata,
    Column(
        "cron_id",
        String(26),
        ForeignKey("cron_dag_entries.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("task_id", String(26), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
)

# ---------------------------------------------------------------------------
# TABLE_GROUPS — maps feature name → list of tables for selective migrations
# ---------------------------------------------------------------------------

TABLE_GROUPS: dict[str, list[Table]] = {
    "routing": [roles, queues, role_queues, task_routing],
    "task_state": [tasks, task_queue, task_fan_in, dag_runs],
    "dead_letter": [dead_letter_queue],
    "task_schedule": [task_schedule],
    "cron_dag": [cron_dag_entries, cron_dag_active_runs],
}
