# Routing Backend Comparison

The routing backend controls where queue, role, and task-routing config is stored. Select via the `ROUTING_BACKEND` environment variable. The four options have meaningfully different infrastructure requirements, consistency guarantees, and operational tradeoffs.

## Feature matrix

| Capability | `static` | `sql` | `redis` | `redis_json` |
| --- | --- | --- | --- | --- |
| Queue / role / routing CRUD | Read-only (HTTP 405) | Full | Full | Full |
| SQL dependency | None | Required | None | None |
| Redis Stack required | No | No | No | Yes |
| Config durability | In-process memory (reloaded from file/env) | SQL database | Redis persistence | Redis persistence |
| Atomicity of `delete_queue` | N/A | Full transaction | Two-phase, role-cleanup-first (safe ordering) | Two-phase, role-cleanup-first (safe ordering) |
| `bump_refresh_tags_for_queue` cost | N/A (raises) | O(SQL join) | O(N roles), pipelined | O(indexed query) |
| Schema migrations | None | Auto-run at startup | None | RediSearch indexes created at startup |
| Runtime config changes | No | Yes | Yes | Yes |

---

## `static`

**Best for:** deployments where queue topology is fixed at deploy time — analogous to Celery's broker routing.

### Strengths

- Zero database dependencies. Only Redis is needed (for task queuing); routing config lives in process memory.
- Config is loaded once from a JSON/YAML file (`STATIC_CONFIG_FILE`). Nothing to manage at runtime.
- Cheapest possible read path: in-process dictionary lookup with no network round-trips.

### Gaps and quirks

- All write operations on queues, roles, and routing configs raise `RoutingBackendReadOnlyError`, surfaced as HTTP 405.
- The refresh tag is a single ULID set at construction and never changes. Workers will not re-poll their queue config during the lifetime of the process. There is no way to trigger a worker refresh without restarting.

---

## `sql` (default)

**Best for:** production deployments where config durability and runtime CRUD via the API matter.

### Strengths

- Full ACID transactions for all writes. Every queue, role, and routing config change is atomic.
- Persistent by default.
- Schema migrations run automatically at startup; schema drift is tracked.
- `delete_queue` is a single transaction that cascades to `role_queues` and bumps refresh tags atomically.
- `bump_refresh_tags_for_queue` uses a SQL JOIN — efficient regardless of role count.

### Gaps

- Adds a SQL database as an infrastructure dependency. The default (`sqlite+aiosqlite:///jobbers.db`) is single-writer and unsuitable for multi-process deployments without care. Multi-process production use needs Postgres via `SQL_PATH`.
- If the SQL database is unavailable at startup, the process fails — there is no fallback to a cached config.

---

## `redis`

**Best for:** dynamic config without SQL, small-to-moderate numbers of roles.

### Strengths

- No SQL dependency, no Redis Stack modules required — works with any standard Redis instance.
- Full CRUD for queues, roles, and routing configs.
- Transactional pipelines for most writes.

### Gaps

- **`bump_refresh_tags_for_queue` is O(N roles):** all N membership checks are issued in a single pipelined round trip, but the work is still proportional to role count. Fine for ≤50 roles; a concern at scale (where `redis_json`'s indexed query has the advantage).
- **`delete_queue` is two-phase but uses safe ordering:** role cleanup (SREM from every role set + refresh tag bumps) runs first in a pipeline, then the queue config key is deleted. A crash between phases leaves an orphaned-but-valid queue config rather than roles referencing a deleted queue. The role-cleanup phase itself is all-or-nothing: all `SREM` calls are issued in a single pipeline, then tag bumps follow in a single MULTI/EXEC.
- **Config durability depends on Redis persistence:** without AOF or RDB configured, a Redis restart wipes all routing config. Config must be re-created via the API after every restart.
- No migration mechanism — a key naming change requires manual cleanup.

---

## `redis_json`

**Best for:** large deployments with many roles and queues, already running Redis Stack.

### Strengths

- RediSearch indexes (`routing_queue_idx`, `routing_role_idx`) are created at startup via `ensure_indexes()`, mirroring SQL schema migrations — no per-request lazy initialization.
- `bump_refresh_tags_for_queue` and `get_all_roles` use the role index — role enumeration scales without scanning all roles.
- `get_all_queues` uses the queue index — no secondary index set to maintain.
- Config stored as JSON documents, which are easy to inspect and debug in Redis directly.
- Full CRUD supported.

### Gaps

- **Requires Redis Stack** (RedisJSON + RediSearch modules). Not available in standard Redis distributions; requires Redis Stack, Redis Enterprise, or a managed service. Tests that depend on this backend are skipped when Redis Stack is unavailable.
- **`delete_queue` is two-phase but uses safe ordering:** role cleanup (removing the queue from all affected role docs + bumping refresh tags) runs first in a single MULTI/EXEC pipeline, then the queue config key is deleted. A crash between phases leaves an orphaned-but-valid queue config rather than roles with dangling references.
- Same durability caveat as `redis`: config is lost on Redis restart without persistence enabled.
- Role queue membership is stored as a JSON array rather than a Redis set, so `get_queues` round-trips through JSON deserialization instead of a native `SMEMBERS`.

---

## Cross-cutting gaps

**No cross-backend data migration.** Switching from `sql` to `redis` (or any other combination) requires re-creating all queues, roles, and routing configs via the API. There is no migration tool.

**Redis keyspace sharing.** Both `redis` and `redis_json` store routing config (`config:*` / `routing:*` keys) on the same Redis instance as task data (`task:*`, `task-queues:*`, etc.). An aggressive Redis eviction policy (e.g., `allkeys-lru`) can evict routing config under memory pressure, which silently breaks queue and role reads.

**Pub/sub refresh is backend-agnostic.** The Redis pub/sub notification (`queue-config-refresh:{role}`) that triggers immediate worker refresh is published in `task_routes.py` at the call site, not inside the backend implementations. All four backends benefit equally from it on write operations — except `static`, which cannot publish because writes raise errors.
