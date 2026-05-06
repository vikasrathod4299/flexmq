# flexmq

`flexmq` is a lightweight TypeScript job queue for Node.js.

It gives you typed `Queue<T>` and `Worker<T>` primitives with retries, backpressure control, worker concurrency, lease-based recovery, and an easy path from in-memory development to Redis or Postgres-backed production.

## Why use flexmq

- start locally with in-memory storage and zero infrastructure
- keep the same queue and worker API when you move to Redis or Postgres
- choose explicit backpressure behavior instead of hidden queue-full behavior
- run reliable at-least-once background jobs with recovery semantics built in

## Installation

```bash
npm install flexmq
```

## Quick start

```ts
import { Queue, Worker } from "flexmq";

type JobPayload = { message: string };

const queue = new Queue<JobPayload>("emails", {
  capacity: 1000,
});

const worker = new Worker<JobPayload>("emails", {
  concurrency: 2,
  processor: async (job) => {
    console.log("Processing:", job.payload.message);
  },
});

async function main() {
  await queue.connect();

  await queue.add({ message: "Welcome email" }, { maxAttempts: 3 });
  await queue.add({ message: "Password reset" }, { maxAttempts: 5 });

  await worker.start();
}

main().catch(console.error);
```

## Best fit

Use `flexmq` when you need:

- background jobs inside a TypeScript app
- retries and failure handling without building queue plumbing yourself
- a queue library that can scale from local dev to multi-worker production
- flexibility to choose Redis or Postgres later through adapters

Common workloads:

- emails and notifications
- webhooks and outbound API retries
- async data processing
- reports, exports, and document generation

## Core concepts

- `Queue<T>`: accepts jobs and applies backpressure strategy when full
- `Worker<T>`: consumes jobs with configurable concurrency
- `StorageAdapter<T>`: contract for Redis, Postgres, or custom backends
- `BackpressureStrategy`: queue-full behavior control

## Backpressure strategies

Use `backpressureStrategy` in `Queue` options:

- `BackpressureStrategy.BLOCK_PRODUCER` (default)
- `BackpressureStrategy.DROP_OLDEST`
- `BackpressureStrategy.DROP_NEWEST`
- `BackpressureStrategy.ERROR`

Current semantics:

- queue capacity applies to `pending` jobs only
- `BLOCK_PRODUCER` waits until pending capacity is available
- FIFO fairness for blocked producers is per `Queue` instance

Example:

```ts
import { Queue, BackpressureStrategy } from "flexmq";

const queue = new Queue("events", {
  capacity: 500,
  backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
});
```

## Retries and delivery semantics

Set retries per job with `maxAttempts`:

```ts
await queue.add({ message: "Send receipt" }, { maxAttempts: 4 });
```

On processor errors:

- job is retried with exponential backoff
- after max attempts, job is marked `failed`

Delivery model:

- delivery guarantee is `at-least-once`
- completion is the durable ack boundary
- duplicate delivery is possible after lease expiry or retry recovery
- processors should be idempotent when required by the workload

## Claim and lease model

- jobs move through `pending -> processing -> completed | delayed | failed`
- claimed jobs carry `workerId`, `claimedAt`, `leaseUntil`, and `claimToken`
- only the active claim owner may mutate a processing job
- expired claims are recoverable

## Waiting for terminal state

- `queue.waitForTerminalState(jobId, timeoutMs)` waits for `completed` or `failed`
- it returns the terminal `Job<T>` or `null` on timeout
- this is useful when a producer wants to observe job outcome without manual polling

## Adapters

- in-memory adapter: built in
- Redis adapter: [`@flexmq/redis`](https://www.npmjs.com/package/@flexmq/redis)
- Postgres adapter: `@flexmq/postgres`

### Redis quick install

```bash
npm install flexmq @flexmq/redis ioredis
```

### Postgres quick install

```bash
npm install flexmq @flexmq/postgres pg
```

## Events

### Queue events

- `queue:connected`
- `queue:disconnected`
- `job:added` (emits the job object)
- `job:dropped`

### Worker events

- `worker:started`
- `worker:stopped`
- `job:processing`
- `job:completed`
- `job:retry`
- `job:failed`
- `job:lost-claim`
- `jobs:promoted`
- `jobs:recovered`
- `worker:error`

## API surface

Main exports include:

- `Queue`
- `Worker`
- `BackpressureStrategy`
- `StorageAdapter`
- `MemoryStorageAdapter`
- `getMemoryStorage()`
- `clearMemoryStorageRegistry()`

## Requirements

- Node.js >= 16

## License

MIT
