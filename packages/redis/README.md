# @flexmq/redis

Redis storage adapter for [`flexmq`](https://www.npmjs.com/package/flexmq).

It provides persistent queue storage, delayed job scheduling, lease-based recovery, and safe multi-worker coordination using Redis + Lua scripts.

## Installation

```bash
npm install flexmq @flexmq/redis ioredis
```

## Requirements

- Node.js `>=16`
- Redis `>=6`
- `ioredis` `^5`

## Quick start

```ts
import { Queue, Worker } from "flexmq";
import { RedisStorageAdapter } from "@flexmq/redis";

type Payload = { message: string };

const storage = new RedisStorageAdapter<Payload>({
  host: "127.0.0.1",
  port: 6379,
  password: process.env.REDIS_PASSWORD,
  capacity: 1000,
});

const queue = new Queue<Payload>("emails", {
  storage,
  capacity: 1000,
});

const worker = new Worker<Payload>("emails", {
  storage,
  concurrency: 2,
  processor: async (job) => {
    console.log("Processing:", job.payload.message);
  },
});

async function main() {
  await storage.connect();
  await queue.connect();

  await queue.add({ message: "Welcome email" }, { maxAttempts: 3 });
  await queue.add({ message: "Password reset" }, { maxAttempts: 5 });

  await worker.start();
}

main().catch(console.error);
```

## Configuration

`RedisStorageAdapter` accepts a Redis config object (typed in package exports).  
Common fields:

- `host`
- `port`
- optional `password`
- `capacity`

Queue name is provided to `Queue` and `Worker`, not to the Redis config.

## Runtime behavior

- Jobs are stored in Redis hashes.
- Pending jobs are consumed with claim/lease semantics.
- Delayed jobs are promoted when due.
- Expired processing leases can be recovered back to pending.
- Payload is serialized/deserialized safely for object payloads.
- Processing ownership is enforced with `claimToken` validation.
- `BLOCK_PRODUCER` wakeups use Redis Streams for cross-process notification.

## Delivery and capacity semantics

- delivery guarantee is `at-least-once`
- queue capacity currently applies to `pending` jobs only
- a producer blocked by `BLOCK_PRODUCER` wakes when pending capacity is freed
- blocked producer fairness is FIFO per queue instance, not global across all processes
- `waitForTerminalState()` uses Redis Streams to observe terminal job transitions across processes

## Production notes

- Use a dedicated Redis DB per environment until prefix support is added.
- Run multiple workers for horizontal scaling.
- Monitor retry/failure rates.
- Ensure clocks are reasonably synchronized across worker machines.
- Use graceful shutdown in workers to avoid duplicate processing windows.

## Claim lifecycle

- `claim()` atomically moves a job from `pending` to `processing`
- claimed jobs store `workerId`, `claimedAt`, `leaseUntil`, and `claimToken`
- `complete()`, `fail()`, `retry()`, and `renewLease()` verify `claimToken`
- `recoverExpiredJobs()` restores expired claims to `pending`

## Terminal state waiting

- `queue.waitForTerminalState(jobId, timeoutMs)` returns when a job reaches `completed` or `failed`
- Redis uses stream-backed lifecycle events so the producer can wait across processes
- timeout returns `null`

## Related packages

- Core queue: [`flexmq`](https://www.npmjs.com/package/flexmq)

## License

MIT
