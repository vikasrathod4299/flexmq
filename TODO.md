# Production Readiness TODO

This file tracks the work needed to make `flexmq` production-grade, especially for distributed workers and a future Postgres adapter.

The main goal is to make queue semantics explicit and correct before adding feature breadth.

## Must-Have

### 1. Define ack / complete / fail semantics

Problem definition:
- The queue does not clearly define when a claimed job is considered successfully finished.
- It is not explicit whether `markCompleted` is the logical ack, or whether a stronger claim/ack contract is required.
- Without a clear ack model, crash recovery and delivery guarantees remain ambiguous.

Approach:
- Treat job completion as an explicit state transition after a worker has successfully processed a claimed job.
- Define the lifecycle contract clearly: `pending -> processing -> completed | delayed | failed`.
- Define what counts as a successful ack, what counts as failure, and what happens if the worker dies before confirming terminal state.

Steps:
- [ ] Write the lifecycle contract for claim, ack/complete, fail, retry, and recover
- [ ] Define whether `markCompleted` is the official ack operation or whether a dedicated `ack` API is needed
- [ ] Define worker ownership rules for who is allowed to complete or fail a claimed job
- [ ] Document what happens if a worker crashes after claim but before terminal update
- [ ] Add tests covering ack success, fail path, and crash-before-ack path

### 2. Make Redis lifecycle transitions atomic and safe

Problem definition:
- Redis retry behavior is currently unsafe because a job can remain in `processing` while also being scheduled for retry.
- That can create duplicate execution windows and inconsistent recovery behavior.

Approach:
- Make every lifecycle transition atomic in storage.
- A job should never exist in conflicting states at the same time.
- Retry scheduling must remove the job from `processing`, clear active lease metadata, and move it into delayed state in one operation.

Steps:
- [ ] Audit all Redis lifecycle transitions: enqueue, claim, complete, fail, retry, recover
- [ ] Add atomic transition logic for `processing -> delayed`
- [ ] Add atomic transition logic for `processing -> completed`
- [ ] Add atomic transition logic for `processing -> failed`
- [ ] Ensure recover only operates on genuinely stale claimed jobs
- [ ] Add integration tests proving a job cannot be both delayed and processing

### 3. Add lease / visibility timeout semantics

Problem definition:
- Multi-worker processing requires a claimed job to be temporarily hidden from others while still being reclaimable if the worker dies.
- Current semantics do not clearly define `claimedAt`, `leaseUntil`, or visibility timeout behavior.

Approach:
- Add explicit lease metadata to claimed jobs.
- A worker claim should reserve the job until a lease expires.
- If the worker renews the lease, the job stays owned; if the lease expires, the job becomes recoverable.

Steps:
- [ ] Define the lease model: `claimedAt`, `leaseUntil`, `workerId`, heartbeat timestamp
- [ ] Update the storage contract to support lease-aware claim and renewal behavior
- [ ] Add heartbeat / lease renewal support in workers
- [ ] Ensure claims are invisible to other workers until lease expiry
- [ ] Add tests for long-running jobs with active lease renewal
- [ ] Add tests for lease expiry and reclaim behavior

### 4. Add crash recovery semantics

Problem definition:
- A production queue must define what happens when a worker dies while a job is in progress.
- Right now recovery is not clearly specified as an ongoing system behavior.

Approach:
- Define stale-job detection as a core system responsibility.
- Recovery should be continuous, not only on worker startup.
- Recovery should be based on expired leases, not only on elapsed processing time.

Steps:
- [ ] Define stale-job detection rules
- [ ] Add continuous recovery loop for expired claims
- [ ] Ensure recovered jobs return to the correct state (`pending` or delayed retry path)
- [ ] Define how quickly recovery should happen after lease expiry
- [ ] Add crash/restart integration tests
- [ ] Add tests for recovery under multiple workers

### 5. Fix timeout API naming and units

Problem definition:
- Timeout naming is confusing because `timeoutMs` is used in places where the underlying storage timeout behaves in seconds.
- This creates incorrect expectations and subtle production bugs.

Approach:
- Standardize timeout units across core and adapters.
- Use clear names such as `dequeueTimeoutMs` or `pollTimeoutMs`.
- Convert units explicitly at adapter boundaries.

Steps:
- [ ] Audit all timeout-related fields and arguments
- [ ] Rename timeout fields to reflect actual units
- [ ] Normalize unit conversions between worker, memory adapter, and Redis adapter
- [ ] Update tests to verify timeout behavior precisely
- [ ] Update docs and examples to match final units

### 6. Define delivery guarantee explicitly

Problem definition:
- The queue does not clearly state whether delivery is at-most-once, at-least-once, or exactly-once.
- Without this, users cannot safely design idempotent consumers.

Approach:
- Adopt `at-least-once` as the primary production guarantee.
- Document duplicate-delivery expectations and required idempotency at the consumer level.
- Tie the guarantee to atomic claim, lease expiry, recovery, and terminal state handling.

Steps:
- [ ] Decide the official delivery guarantee
- [ ] Document why that guarantee is achievable in each adapter
- [ ] Document duplicate delivery windows and failure cases
- [ ] Add tests that demonstrate the guarantee under crash and retry scenarios

### 7. Define delayed scheduling semantics

Problem definition:
- `nextAttemptAt` exists, but delayed execution semantics are not fully defined.
- It is unclear how normal delayed jobs, retries, and promotion timing should behave.

Approach:
- Define delayed jobs as jobs that are not claimable until their scheduled time.
- Use a clear scheduler/promotion path in storage.
- Ensure delayed retries and intentionally delayed jobs follow the same contract.

Steps:
- [ ] Define how delayed jobs are stored and promoted
- [ ] Define whether dequeue must skip future-scheduled jobs or rely on promotion only
- [ ] Define polling/promotion cadence and wake-up strategy
- [ ] Ensure delayed retry path uses the same durable scheduling rules
- [ ] Add tests for delayed jobs, retries, and promotion timing

### 8. Define dead-letter and final-failure semantics

Problem definition:
- After `maxAttempts`, the final handling policy is not explicit enough for production use.
- Users need predictable behavior for failed jobs.

Approach:
- Define final failure as a durable terminal state.
- Add dead-letter queue support or, at minimum, durable failed-job retention and inspection.
- Make requeue-from-failure a deliberate administrative action.

Steps:
- [ ] Define final-failure policy after max attempts
- [ ] Decide whether failed jobs remain in place or move to a DLQ
- [ ] Add failed job retention and inspection behavior
- [ ] Add DLQ and requeue-from-DLQ flow if chosen
- [ ] Add tests for permanent failure behavior

### 9. Define ordering semantics

Problem definition:
- Ordering rules are not clearly documented.
- Production users need to know whether the queue is strict FIFO, FIFO among ready jobs, or best-effort ordering.

Approach:
- Define ordering only for claimable jobs.
- Clarify how retries and delayed promotions interact with FIFO ordering.
- Avoid overpromising strict global ordering if delayed jobs and retries can reorder work.

Steps:
- [ ] Define ordering guarantee for pending ready-to-run jobs
- [ ] Define how retries re-enter the queue
- [ ] Define how delayed promotions affect ordering
- [ ] Document whether ordering is strict or best-effort under concurrency
- [ ] Add tests for ordering behavior under retry and delayed scenarios

### 10. Add reliable cross-process completion/failure observation

Problem definition:
- Current lifecycle events are local `EventEmitter` events and are not sufficient for distributed producers or external observers.
- Polling storage works but is not enough for a strong lifecycle API.

Approach:
- Keep storage state as the source of truth.
- Add durable lifecycle notifications for cross-process observation.
- Prefer Redis Streams for Redis and a durable `job_events` table for Postgres.
- Do not rely on Pub/Sub alone for correctness.

Steps:
- [ ] Define durable lifecycle event model (`processing`, `retry`, `completed`, `failed`, `recovered`)
- [ ] Add durable terminal-state notification support to Redis adapter
- [ ] Design Postgres equivalent using a durable event log table
- [ ] Add producer-side API such as `waitForTerminalState(jobId)`
- [ ] Keep optional Pub/Sub / `LISTEN/NOTIFY` only as wake-up hints
- [ ] Add integration tests for distributed observers

### 11. Make `BLOCK_PRODUCER` correct or redefine it

Problem definition:
- Current producer blocking behavior is not properly coupled to freed capacity, especially across processes.
- This can leave blocked producers waiting indefinitely.

Approach:
- Decide whether `BLOCK_PRODUCER` is only an in-process feature or a distributed queue feature.
- If distributed, it must be backed by durable capacity-change observation, not just local memory.
- If not worth the complexity, scope it down clearly.

Steps:
- [ ] Decide whether `BLOCK_PRODUCER` is supported for distributed adapters
- [ ] If yes, design durable unblock behavior when capacity is freed
- [ ] If no, document in-process-only limitations clearly
- [ ] Add tests for unblock behavior in both supported modes

### 12. Add real Redis integration and failure-mode coverage

Problem definition:
- Mock-based tests are useful, but they do not prove production behavior under real concurrency or crash scenarios.
- Production confidence requires end-to-end verification against real Redis.

Approach:
- Add integration tests against an actual Redis instance.
- Cover failure modes, restarts, concurrency, and duplicate-delivery windows.

Steps:
- [ ] Add real Redis integration test setup
- [ ] Add tests for retry and delayed transitions
- [ ] Add tests for worker crash before terminal update
- [ ] Add tests for lease expiry and stale job recovery
- [ ] Add multi-worker concurrency tests
- [ ] Add reconnect and temporary Redis failure tests

## Should-Have

### 13. Add administrative job control APIs

Problem definition:
- Production support workflows need more than enqueue/dequeue.
- Operators need explicit controls to inspect and manipulate jobs safely.

Approach:
- Expose administrative APIs around inspection and controlled job mutation.

Steps:
- [ ] Add retry-by-id
- [ ] Add cancel/remove job
- [ ] Add purge queue
- [ ] Add pause/resume queue or worker
- [ ] Add inspection APIs by state

### 14. Add worker health/readiness and operational metrics

Problem definition:
- Process-local metrics are not enough for real operations.
- Operators need health, readiness, lag, queue depth, and failure visibility.

Approach:
- Add backend-aware operational reporting and simple health/readiness signals.

Steps:
- [ ] Add worker health/readiness checks
- [ ] Expose queue counts by state
- [ ] Add retry/failure/processing metrics derived from backend truth
- [ ] Add structured logging hooks

### 15. Add namespace / prefix support for adapters

Problem definition:
- Production environments need isolation across apps, stages, and tenants.
- Redis adapter currently lacks clear namespace/prefix configuration.

Approach:
- Add adapter-level namespace support and document naming strategy.

Steps:
- [ ] Add key prefix / namespace support to Redis config
- [ ] Plan equivalent namespace support for Postgres tables/queue names
- [ ] Add tests for isolated environments sharing one backend

### 16. Align docs, types, and package metadata with actual behavior

Problem definition:
- Public contract mismatches create production risk.
- Docs and type definitions should never overclaim behavior.

Approach:
- Make docs, runtime payloads, and type contracts consistent.

Steps:
- [ ] Align typed event contracts with actual emitted payloads
- [ ] Remove inaccurate package metadata claims
- [ ] Update README examples to reflect final semantics
- [ ] Document operational caveats clearly

### 17. Add retention and cleanup policy

Problem definition:
- Completed and failed jobs need explicit cleanup behavior to avoid unbounded storage growth.

Approach:
- Add configurable retention by state and safe cleanup routines.

Steps:
- [ ] Define retention policy for completed jobs
- [ ] Define retention policy for failed / DLQ jobs
- [ ] Add cleanup operations and tests

## Nice-To-Have

### 18. Add priority semantics

Problem definition:
- Some workloads need important jobs processed sooner, but priority is not required for a safe v1.

Approach:
- Add explicit priority fields and priority-aware claim ordering only after core reliability is complete.

Steps:
- [ ] Add job priority field
- [ ] Add priority-aware dequeue/claim behavior
- [ ] Define ordering within the same priority band

### 19. Add rate limiting and throttling

### 20. Add deduplication / idempotency key support

### 21. Add repeatable / cron jobs

### 22. Add job result storage and producer-side completion waiting

### 23. Add dashboard / inspection tooling

## Implementation Phases

### Phase 1: Core Semantics and Correctness
- [ ] Ack / complete / fail semantics
- [ ] Atomic lifecycle transitions
- [ ] Lease / visibility timeout semantics
- [ ] Crash recovery semantics
- [ ] Timeout unit cleanup
- [ ] Delivery guarantee documentation
- [ ] Delayed scheduling semantics
- [ ] Dead-letter / final failure semantics
- [ ] Ordering semantics

### Phase 2: Distributed Reliability
- [ ] Durable lifecycle observation
- [ ] Redis Streams design
- [ ] Postgres durable event log design
- [ ] `waitForTerminalState(jobId)` API
- [ ] `BLOCK_PRODUCER` decision and redesign

### Phase 3: Operational Readiness
- [ ] Admin APIs
- [ ] Health/readiness and backend-derived metrics
- [ ] Namespace / prefix support
- [ ] Retention and cleanup
- [ ] Docs/types/package alignment

### Phase 4: Test and Release Hardening
- [ ] Real Redis integration suite
- [ ] Crash/restart tests
- [ ] Lease expiry tests
- [ ] Multi-worker concurrency tests
- [ ] Reconnect / temporary backend failure tests
- [ ] CI for build, lint, unit, and integration tests

### Phase 5: Advanced Features
- [ ] Priority
- [ ] Rate limiting
- [ ] Deduplication
- [ ] Repeatable jobs
- [ ] Result storage
- [ ] Dashboard tooling

## Recommended Build Order

1. [ ] Define lifecycle semantics first
2. [ ] Fix Redis lifecycle correctness
3. [ ] Add lease/heartbeat + continuous recovery
4. [ ] Clean up timeout semantics
5. [ ] Define and document delivery guarantee
6. [ ] Add durable lifecycle observation
7. [ ] Fix or scope down `BLOCK_PRODUCER`
8. [ ] Add dead-letter + admin APIs
9. [ ] Add observability, retention, docs, and CI
10. [ ] Add advanced features only after correctness is proven