"""Central SQLAlchemy DDL definitions for the Jobbers schema."""
from __future__ import annotations

from sqlalchemy import Column, ForeignKey, Index, Integer, MetaData, String, Table

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

Index("idx_roles_refresh_tag", roles.c.refresh_tag)
Index("idx_role_queues_role_queue", role_queues.c.role, role_queues.c.queue)
