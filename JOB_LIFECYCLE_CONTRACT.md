# Job Lifecycle Contract

This document defines the expected behavior of job state transitions in `flexmq`.

It is the behavior contract between:

- `Queue`
- `Worker`
- `StorageAdapter`
- storage implementations such as memory, Redis, and a future Postgres adapter

The goal is to make queue semantics explicit so all adapters behave consistently and production guarantees are clear.

## Why this document exists

Today, some lifecycle behavior is implied by code in workers and storage adapters.
That makes it easy for adapters to drift or behave differently under failures.

This contract defines:

- what each job state means
- which state transitions are allowed
- when a job is considered acknowledged
- what happens on retries, failures, lease expiry, and worker crashes
- what storage adapters must guarantee

## Delivery goal

The target production delivery model is:

- `at-least-once` delivery

This means:

- a job should not be silently lost once accepted by the queue
- a job may be delivered more than once in crash or retry scenarios
- processors must be written to be idempotent when required by the workload

The system does not attempt to guarantee exactly-once delivery.

## State model

Each job must be in exactly one lifecycle state at a time.

Core states:

- `pending`: ready to be claimed by a worker
- `processing`: currently claimed by one worker and protected by an active lease
- `delayed`: scheduled for future execution and not yet claimable
- `completed`: terminal success state
- `failed`: terminal failure state

Rules:

- a job must never be in both `processing` and `delayed`
- a job must never be both terminal and claimable
- all state transitions must be durable before they are considered complete

## Required job metadata

To support reliable distributed execution, a claimed job should carry the following metadata:

- `attempts`
- `maxAttempts`
- `workerId`
- `claimedAt`
- `leaseUntil`
- `nextAttemptAt`
- `error`
- `createdAt`
- `updatedAt`

Notes:

- `claimedAt` is when the current worker claim began
- `leaseUntil` is when the current claim expires if not renewed
- `nextAttemptAt` is the earliest time a delayed job becomes eligible again

## Lifecycle transitions

The allowed transitions are:

- `pending -> processing`
- `processing -> completed`
- `processing -> delayed`
- `processing -> failed`
- `processing -> pending` only through recovery after lease expiry
- `delayed -> pending` when promotion time is reached

Transitions that should not happen directly:

- `pending -> completed`
- `pending -> failed`
- `delayed -> processing` without becoming claimable through the adapter's scheduling rules
- `completed -> pending`
- `failed -> pending` unless an explicit admin requeue operation is introduced later

## Claim semantics

Claiming a job means a worker atomically takes ownership of a pending job.

When a job is claimed:

- it moves from `pending` to `processing`
- `attempts` is incremented according to the adapter's lifecycle rules
- `workerId` is stored
- `claimedAt` is set
- `leaseUntil` is set
- the job becomes invisible to other workers until lease expiry

Claim requirements:

- only one worker may successfully claim a given job at a time
- claim must be atomic
- claim visibility must be durable before the job is handed to the processor

## Ack and completion semantics

In this codebase, "ack" means the job has been durably transitioned out of `processing` into a terminal success state.

A job is not considered acknowledged when:

- it is only returned by `dequeue`
- the processor starts running
- the processor returns successfully in memory but storage has not yet been updated

A job is considered acknowledged only when:

- storage durably records `processing -> completed`

This means:

- successful processor execution alone is not enough
- the ack boundary is the durable storage update

If a dedicated `ack()` API is not introduced, then `markCompleted()` is the logical ack operation.

## Failure and retry semantics

If job processing throws an error:

- if `attempts < maxAttempts`, the job should transition to `delayed`
- if `attempts >= maxAttempts`, the job should transition to `failed`

Retry rules:

- retry scheduling must be atomic
- retry scheduling must remove the job from `processing`
- retry scheduling must clear or replace active lease metadata
- retry scheduling must set `nextAttemptAt`
- a retried job must not still appear as actively claimed

Final failure rules:

- final failure is a durable terminal state
- failed jobs must remain inspectable unless a later retention policy removes them
- dead-letter queue support may extend this model, but failure must still be explicit and durable

## Delayed job semantics

Delayed jobs are jobs that cannot be claimed before their scheduled time.

Rules:

- a delayed job remains in `delayed` until `nextAttemptAt <= now`
- delayed jobs are not visible to normal claim operations while still scheduled for the future
- promotion from `delayed -> pending` must be durable
- once promoted, the job becomes eligible for normal claiming

This applies to:

- intentionally delayed jobs
- retry backoff scheduling

## Lease and visibility timeout semantics

Every claimed job should have an active lease.

Lease rules:

- the worker that claimed the job owns it until `leaseUntil`
- while the lease is active, no other worker may claim it
- the owning worker may renew the lease while processing a long-running job
- if the lease expires before completion or failure is recorded, the job becomes stale and recoverable

This is the core mechanism that protects multi-worker safety.

## Crash recovery semantics

If a worker dies after claiming a job but before writing a terminal state:

- the job remains in `processing` until lease expiry
- once the lease has expired, the job is considered stale
- a recovery process may move the job back to `pending` or another appropriate retry state

Recovery requirements:

- stale-job detection must be based on lease expiry
- recovery should be continuous, not startup-only
- recovery must not affect jobs with active leases
- recovery must be safe under multiple workers running at the same time

## Ordering semantics

The queue should only guarantee ordering among claimable ready jobs.

Recommended rule:

- FIFO applies to `pending` jobs that are ready to run

Non-goals:

- strict global ordering across delayed jobs, retries, and concurrent workers

Implications:

- retries may re-enter later and change observed execution order
- delayed promotion may change overall order relative to jobs added later
- under concurrency, completion order is not guaranteed even if claim order is FIFO

## Cross-process observation semantics

Local `EventEmitter` events are not sufficient for distributed lifecycle observation.

The source of truth must be durable storage state.

For cross-process observation:

- Redis should use durable lifecycle eventing such as Redis Streams
- Postgres should use a durable `job_events` table
- Pub/Sub or `LISTEN/NOTIFY` may be used only as wake-up hints

Required property:

- observers must be able to reconnect and continue observing without losing lifecycle events

## Storage adapter requirements

Every `StorageAdapter` implementation must satisfy these rules:

- state transitions are durable and adapter-consistent
- claim is atomic
- only one worker can own a claim at a time
- delayed jobs are not claimable early
- retry transitions cannot leave jobs in conflicting states
- stale claimed jobs can be safely recovered
- terminal states are durable and inspectable

Adapters may differ in implementation details, but not in lifecycle meaning.

## How this applies to current adapters

### Memory adapter

- can model the same lifecycle in a simplified in-process way
- should still follow the same state and ownership rules where applicable

### Redis adapter

- should enforce lifecycle transitions atomically
- should use durable structures for claim, delay, retry, recovery, and lifecycle observation

### Future Postgres adapter

- should use row-level claiming such as `FOR UPDATE SKIP LOCKED`
- should use lease-aware processing state
- should use a durable event log table for lifecycle observation

## Test expectations

This contract should be enforced by tests that prove:

- only one worker can claim a job at a time
- a successful processor run is not considered done until storage confirms completion
- failed jobs retry or fail according to attempt rules
- delayed jobs are not claimable early
- stale jobs are recovered after lease expiry
- crash-before-ack scenarios do not lose jobs
- duplicate-delivery windows match the documented at-least-once model

## Short version

If you need the practical summary, it is this:

- claim must be atomic
- complete is the ack
- retry must leave `processing` cleanly
- delayed jobs must not be claimable early
- stale claimed jobs must be recoverable
- the system guarantees at-least-once delivery, not exactly-once
