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

Index("idx_roles_refresh_tag", roles.c.refresh_tag)
Index("idx_role_queues_role_queue", role_queues.c.role, role_queues.c.queue)

cron_dag_entries = Table(
    "cron_dag_entries",
    metadata,
    Column("id", String, primary_key=True),              # str(ULID)
    Column("name", String, nullable=False),
    Column("cron_expr", String, nullable=False),
    Column("dag_spec", String, nullable=False),           # JSON text
    Column("enabled", Boolean, nullable=False),
    Column("concurrency_policy", String, nullable=False),
    Column("created_at", String, nullable=False),         # ISO 8601
    Column("next_run_at", String, nullable=True),         # ISO 8601; NULL when acquired
)

cron_dag_active_runs = Table(
    "cron_dag_active_runs",
    metadata,
    Column("cron_id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("expires_at", String, nullable=False),         # ISO 8601
)

Index("idx_cron_dag_entries_next_run_at", cron_dag_entries.c.next_run_at)
