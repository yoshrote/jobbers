# Codebase Review — 2026-07

Full-backend review (not a single-PR diff) covering `jobbers/` in its entirety: core task lifecycle, HTTP/API layer and process entry points, Redis/RedisJSON adapters, SQL/static adapters and migrations, and models/shared utilities. Findings are ranked by severity and deduplicated across reviewers where the same defect was found from multiple angles.



## Medium — performance / operational

- **`jobbers/adapters/redis/cron_dag_scheduler.py:174-196`** — the active-run marker used for `SKIP_IF_RUNNING` has a hardcoded 24h TTL with no renewal. A DAG run genuinely longer than 24h has its marker expire mid-flight, and the next cron tick dispatches a duplicate concurrent run.

## Notable pattern

All eight Critical findings and all High-severity findings are now fixed and verified. Of the Medium findings, four of five are fixed; `jobbers/adapters/redis/cron_dag_scheduler.py`'s hardcoded 24h active-run TTL (no renewal mechanism) was explicitly deferred, as it requires a real design decision (a renewal sweep, most likely via the Cleaner process) rather than a mechanical fix.

Two recurring lessons emerged across the fixes: (1) the codebase's saga/mock-backed tests (`DummyTaskState`/`DummyTaskSubmit`) don't replicate real backend semantics (Lua-script guards, sliding-window pruning, row-lock behavior) precisely enough to catch every regression on their own — several fixes added real-backend adapter-contract tests specifically to close that gap; (2) several fixes required distinguishing operations that look similar but need different atomicity/idempotency semantics — a single primitive reused for both was the root cause each time, not the concurrency handling itself being absent.