# flexmq

`flexmq` is a lightweight TypeScript job queue with pluggable storage.

The project is organized as a monorepo with three packages:

- `flexmq` — core queue and worker implementation (in-memory storage included)
- `@flexmq/redis` — Redis storage adapter for production-style deployments
- `@flexmq/postgres` — Postgres storage adapter for SQL-backed deployments

It is designed for:

- background job processing
- retry with exponential backoff
- configurable backpressure behavior
- worker concurrency
- lease-based recovery for expired claims

## Packages

### `flexmq` (core)

Main primitives:

- `Queue<T>`
- `Worker<T>`
- `StorageAdapter<T>`
- `BackpressureStrategy`

### `@flexmq/redis`

Adds `RedisStorageAdapter<T>` that implements the core `StorageAdapter<T>` contract.

### `@flexmq/postgres`

Adds `PostgresStorageAdapter<T>` that implements the core `StorageAdapter<T>` contract.

---

## Install

### In this monorepo

```bash
npm install
```

Build all packages:

```bash
npm run build
```

Run tests:

```bash
npm test
```

---

## Quick start (in-memory)

```ts
import { Queue, Worker } from "flexmq";

type EmailJob = { to: string; subject: string };

const queue = new Queue<EmailJob>("emails", { capacity: 1000 });

const worker = new Worker<EmailJob>("emails", {
  concurrency: 2,
  processor: async (job) => {
    // your job logic
    console.log(`Sending email to ${job.payload.to}`);
  },
});

async function main() {
  await queue.connect();

  await queue.add({ to: "user@example.com", subject: "Welcome" }, { maxAttempts: 3 });
  await queue.add({ to: "ops@example.com", subject: "Daily report" }, { maxAttempts: 5 });

  await worker.start();

  // later, when shutting down:
  // await worker.stop();
  // await queue.disconnect();
}

main().catch(console.error);
```

---

## Backpressure strategies

When queue capacity is reached, choose behavior via `backpressureStrategy`:

- `BackpressureStrategy.BLOCK_PRODUCER` (default): wait until pending capacity is available
- `BackpressureStrategy.DROP_OLDEST`: remove oldest pending job, enqueue new one
- `BackpressureStrategy.DROP_NEWEST`: reject newest incoming job
- `BackpressureStrategy.ERROR`: throw immediately

Notes:

- queue capacity currently applies to `pending` jobs only
- with Redis storage, `BLOCK_PRODUCER` wakes blocked producers across processes using Redis Streams
- FIFO fairness for blocked producers is preserved within one `Queue` instance, not globally across all processes

Example:

```ts
import { Queue, BackpressureStrategy } from "flexmq";

const queue = new Queue("events", {
  capacity: 500,
  backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
});
```

---

## Retries and failure handling

- `maxAttempts` is set per job (`queue.add(payload, { maxAttempts })`)
- failed jobs are retried with exponential delay
- once attempts are exhausted, job is marked `failed`

## Delivery semantics

- delivery model is `at-least-once`
- a job is acknowledged only when storage durably records `processing -> completed`
- jobs may be delivered more than once after crashes, retries, or lease expiry recovery
- processors should be idempotent when required by the workload

## Claim and lease model

- workers claim jobs atomically from `pending`
- a claimed job moves to `processing` and gets `workerId`, `claimedAt`, `leaseUntil`, and `claimToken`
- only the worker holding the active `claimToken` may `complete`, `fail`, `retry`, or `renewLease`
- expired claims are recoverable back to `pending`

## Waiting for terminal state

- `queue.waitForTerminalState(jobId, timeoutMs)` waits until a job becomes `completed` or `failed`
- it returns the terminal job record, or `null` if the timeout expires first
- with Redis storage, terminal waiting works across processes via Redis Streams

Example:

```ts
const job = await queue.add({ to: "user@example.com", subject: "Welcome" });

const terminalJob = await queue.waitForTerminalState(job.id, 30000);

if (!terminalJob) {
  console.log("Timed out waiting for terminal state");
} else {
  console.log(terminalJob.status, terminalJob.error);
}
```

---

## Redis adapter example

```ts
import { Queue, Worker } from "flexmq";
import { RedisStorageAdapter } from "@flexmq/redis";

type JobPayload = { taskId: string };

const storage = new RedisStorageAdapter<JobPayload>({
  host: "127.0.0.1",
  port: 6379,
  password: process.env.REDIS_PASSWORD,
  capacity: 10000,
});

const queue = new Queue<JobPayload>("tasks", { storage });
const worker = new Worker<JobPayload>("tasks", {
  storage,
  concurrency: 4,
  processor: async (job) => {
    console.log("Processing", job.payload.taskId);
  },
});
```

---

## Postgres adapter example

```ts
import { Queue, Worker } from "flexmq";
import { PostgresStorageAdapter } from "@flexmq/postgres";

type JobPayload = { taskId: string };

const storage = new PostgresStorageAdapter<JobPayload>({
  host: "127.0.0.1",
  port: 5432,
  user: "postgres",
  password: process.env.PGPASSWORD,
  database: "postgres",
  capacity: 10000,
});

await storage.connect();
await storage.ensureSchema();

const queue = new Queue<JobPayload>("tasks", { storage });
const worker = new Worker<JobPayload>("tasks", {
  storage,
  concurrency: 4,
  processor: async (job) => {
    console.log("Processing", job.payload.taskId);
  },
});
```

---

## Events

Queue emits:

- `queue:connected`
- `queue:disconnected`
- `job:added` (emits the job object)
- `job:dropped`

Worker emits:

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

---

## Benchmarking and reliability

- benchmark guide: `docs/BENCHMARKS.md`
- reliability and stress guide: `docs/RELIABILITY.md`

Included scripts:

```bash
npm run bench:memory
npm run bench:redis
npm run stress:recovery
npm run stress:block-producer
```

---

## Development notes

- TypeScript strict mode enabled
- Jest + ts-jest test setup
- npm workspaces monorepo

Useful commands:

```bash
npm run build
npm run test
npm run test:coverage
npm run clean
```

---

## License

MIT
