"""SQL-specific cron DAG scheduler tests."""

import datetime as dt
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from jobbers.adapters.sql import SQLCronDAGScheduler
from jobbers.models.cron_dag import CronDAGEntry
from jobbers.models.dag import DAGTaskSpec

PAST = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)


def make_entry(name: str = "test_cron", cron_expr: str = "0 * * * *", **kwargs) -> CronDAGEntry:
    return CronDAGEntry(
        name=name,
        cron_expr=cron_expr,
        dag_spec=DAGTaskSpec(name="root_task"),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_list_count_query_does_not_materialize_rows(session_factory):
    """
    list()'s total count is computed via SELECT count(*), not by materializing every row.

    Regression test: an earlier version computed the count via
    len((await session.execute(select(cron_dag_entries)...)).all()), which fetches every
    matching row (including DAG spec JSON blobs) just to count them -- unbounded memory/IO
    growth as the number of cron entries increases. Pinned by inspecting the compiled count
    statement, mirroring the lock-statement assertions already used elsewhere in this suite
    (e.g. test_close_dag_run_task_locks_dag_runs_anchor_not_every_pending_row).
    """
    scheduler = SQLCronDAGScheduler(session_factory)
    for i in range(3):
        await scheduler.add(make_entry(name=f"cron_{i}"), PAST)

    original_execute = AsyncSession.execute
    captured_statements: list[str] = []

    async def _capturing_execute(self, statement, *args, **kwargs):
        captured_statements.append(str(statement))
        return await original_execute(self, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", _capturing_execute):
        entries, total = await scheduler.list()

    assert total == 3
    assert len(entries) == 3
    count_statement = captured_statements[0]
    assert "count(" in count_statement.lower()
    assert "cron_dag_entries.dag_spec" not in count_statement
