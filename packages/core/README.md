# flexmq

A lightweight TypeScript job queue with pluggable storage.

`flexmq` provides `Queue<T>` and `Worker<T>` primitives with retries, backpressure control, delayed retries, and lease-based recovery.  
It ships with an in-memory adapter and allows custom adapters through `StorageAdapter<T>`.

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

  // graceful shutdown example:
  // await worker.stop();
  // await queue.disconnect();
}

main().catch(console.error);
```

## Core concepts

- `Queue<T>`: accepts jobs and applies backpressure strategy when full.
- `Worker<T>`: consumes and processes jobs with configurable concurrency.
- `StorageAdapter<T>`: storage contract for custom adapters.
- `BackpressureStrategy`: full-queue behavior control.

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

## Retries

Set retries per job with `maxAttempts`:

```ts
await queue.add({ message: "Send receipt" }, { maxAttempts: 4 });
```

On processor errors:

- job is retried with exponential backoff
- after max attempts, job is marked `failed`

## Delivery semantics

- delivery guarantee is `at-least-once`
- completion is the durable ack boundary
- duplicate delivery is possible after lease expiry or retry recovery

## Claim and lease model

- jobs move through `pending -> processing -> completed | delayed | failed`
- claimed jobs carry `workerId`, `claimedAt`, `leaseUntil`, and `claimToken`
- only the active claim owner may mutate a processing job
- expired claims are recoverable

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

## Adapters

- In-memory adapter: built in
- Redis adapter: [`@flexmq/redis`](https://www.npmjs.com/package/@flexmq/redis)

### Redis quick install

```bash
npm install flexmq @flexmq/redis ioredis
```

## Requirements

- Node.js >= 16

## License

MIT
