# Redis Adapter Migration Plan

## Goal

Migrate `RedisStorageAdapter` from old API (`dequeue`, `markProcessing`, `markCompleted`, `markFailed`, `scheduleDelayed`, `recoverStuckJobs`) to new claim/lease-based `StorageAdapter<T>` interface.

## Redis Key Structure

- `{queue}:pending` — list (LPUSH/RPOP for FIFO)
- `{queue}:processing` — sorted set, score = `leaseUntil` (enables lease-based recovery)
- `{queue}:delayed` — sorted set, score = `executeAt`
- `{queue}:job:{id}` — hash with all job fields including `claimedAt`, `leaseUntil`, `claimToken`, `workerId`

## New Lua Scripts

### 1. `claim.lua`

- RPOP from pending, set processing state with claimToken/lease/workerId
- ZADD to processing with `leaseUntil` as score
- HINCRBY attempts
- Return job JSON

### 2. `complete.lua`

- Verify claimToken + status=processing
- ZREM from processing
- Update status to completed, clear claim metadata
- EXPIRE jobKey 86400

### 3. `fail.lua`

- Verify claimToken + status=processing
- ZREM from processing
- Update status to failed, set error, clear claim metadata

### 4. `retry.lua`

- Verify claimToken + status=processing
- ZREM from processing
- Set status=delayed, ZADD to delayed set with executeAt score
- Clear claim metadata

### 5. `renew-lease.lua`

- Verify claimToken + status=processing
- Update leaseUntil in hash and ZADD score in processing set

### 6. `recover-expired.lua` (replaces `recoverStuckJobs.lua`)

- ZRANGEBYSCORE processing 0 now (lease-based)
- Reset jobs to pending, clear claim metadata, LPUSH to pending

## Updated Lua Scripts

### `enqueue.lua`

- Add `claimedAt`, `leaseUntil`, `claimToken` as empty string fields

### `promoteDelayed.lua`

- Also set `status` to `pending`

## RedisStorageAdapter.ts Changes

### Remove

- `dequeue()`, `markProcessing()`, `markCompleted()`, `markFailed()`
- `scheduleDelayed()`, `recoverStuckJobs()`, `updateJob()`

### Add

- `claim(queueName, options)` — BRPOP + claim.lua (or RPOP if waitTimeoutMs=0)
- `renewLease(queueName, jobId, claimToken, leaseMs)`
- `complete(queueName, jobId, claimToken)`
- `fail(queueName, jobId, claimToken, error?)`
- `retry(queueName, jobId, claimToken, executeAt, error?)`
- `recoverExpiredJobs(queueName, now)`
- Update `promoteDelayedJobs(queueName, now?)` signature
- Update `parseJobFromRedis()` for new fields
- Update `getJob()` to use updated parser

## Atomicity

Every state transition is a single Lua script = atomic in Redis.
All mutations verify claimToken before proceeding.
