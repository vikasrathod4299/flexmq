Blockers
- Redis retry lifecycle is unsafe: retried jobs are scheduled into delayed state without being cleanly removed from processing, which can cause duplicate execution or premature stuck-job recovery in packages/redis/src/RedisStorageAdapter.ts:140, packages/redis/src/lua-scripts/recoverStuckJobs.lua:8, packages/redis/src/lua-scripts/promoteDelayed.lua:6.
- No proper lease/heartbeat model for distributed workers: a long-running healthy job can be treated as stuck, and recovery only runs once on worker start in packages/core/src/Worker.ts:48.
- BLOCK_PRODUCER is incomplete: blocked producers are not resumed by actual worker completions; tests manually call the private drain path, which exposes the gap in packages/core/src/Queue.ts:153, tests/Queue.test.ts:364.
- Timeout semantics are broken/ambiguous: timeoutMs is named in milliseconds but passed to dequeue APIs that behave in seconds, so the runtime behavior is misleading in packages/core/src/Worker.ts:13, packages/core/src/Worker.ts:34, packages/core/src/storage/MemoryStorageAdapter.ts:105, packages/redis/src/RedisStorageAdapter.ts:82.
- Cross-process ack/fail notification is missing: worker lifecycle events are local EventEmitter events only, so another process cannot reliably know when a job completed or failed except by polling in packages/core/src/Worker.ts:101, packages/core/src/Worker.ts:111, packages/core/src/Worker.ts:129.
Correctness Gaps
- Event typing does not match actual emitted payloads, so the public contract is inconsistent in packages/core/src/types/QueueEvents.ts:3, packages/core/src/Queue.ts:72, packages/core/src/Worker.ts:111.
- markProcessing() exists in the adapter contract but the worker flow largely relies on dequeue side effects; ownership semantics are not clearly modeled in packages/core/src/storage/StorageAdapter.ts:21.
- Metrics are process-local and incomplete for distributed operations; queue metrics are not fully tied to backend truth in packages/core/src/metrics/metrics.ts:28.
- There is no clearly documented delivery guarantee such as at-least-once vs exactly-once, which is critical for production users.
Operational Gaps
- No health/readiness hooks for workers or Redis connectivity.
- No durable lifecycle event log for external consumers/observers.
- No namespacing/prefix support for Redis keys even though docs imply environment isolation in packages/redis/README.md:77, packages/redis/src/RedisConfig.ts:1.
- No CI workflow or real Redis integration validation; Redis tests are mocked, not end-to-end in tests/redis/RedisStorageAdapter.test.ts:1.
Docs / Packaging Gaps
- Docs claim or imply features not actually implemented, including richer Redis config and operational isolation in packages/redis/README.md:61.
- Core package metadata mentions Postgres support, but there is no Postgres adapter in packages/core/package.json:4.
- Public docs don’t explain failure windows, duplicate delivery, or recovery behavior.
Missing Essential Functionality
- Dead-letter queue for permanently failed jobs.
- Explicit lease/heartbeat/visibility-timeout semantics.
- Producer-side wait API for terminal state, like waitForCompletion(jobId) or waitForTerminalState(jobId).
- Durable distributed eventing, ideally Redis Streams rather than plain Pub/Sub.
- Administrative controls: cancel/remove/retry-by-id, pause/resume queue, purge, inspect by state.
- Job result storage or completion payload support.
- Priority queues.
- Rate limiting / throttling.
- Deduplication / idempotency keys.
- Repeatable / cron jobs.
- Retention policies for completed/failed jobs.
- Better observability: per-state counts, retries, worker ownership, processing latency, backend-derived metrics.