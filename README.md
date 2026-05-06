# flexmq

`flexmq` is a lightweight TypeScript job queue for Node.js with one API across in-memory, Redis, and Postgres backends.

Build background jobs locally with zero infrastructure, then keep the same producer and worker code when you move to Redis or Postgres.

## Why teams pick flexmq

- One queue API across memory, Redis, and Postgres
- Built for real background work: retries, backpressure, concurrency, leases, and recovery
- Good fit for SaaS apps, internal tools, APIs, webhooks, email, and report generation
- Lets teams stay on existing infrastructure instead of forcing a Redis-only choice

## Best fit

Use `flexmq` when you want:

- a queue library inside your TypeScript application, not a separate platform
- a smooth path from local development to production
- durable, at-least-once job processing with clear delivery semantics
- the option to run on Redis or Postgres without rewriting your app code

`flexmq` is especially useful for:

- email and notification pipelines
- webhooks and retryable outbound API calls
- async document or report generation
- background data sync and enrichment jobs
- horizontally scaled worker processes

## Choose your backend

| Backend   | Best for                                             | Why use it                                                           |
| --------- | ---------------------------------------------------- | -------------------------------------------------------------------- |
| In-memory | local development, tests, simple single-process apps | zero setup, fastest way to start                                     |
| Redis     | queue-heavy production workloads                     | cross-process coordination, producer wakeups, terminal-state waiting |
| Postgres  | teams already standardized on SQL                    | durable jobs without adding Redis infrastructure                     |

## What stands out

- `Pluggable storage`: start with memory, switch to Redis or Postgres later
- `Backpressure control`: block producers, drop oldest, drop newest, or throw immediately
- `Lease-based claims`: protect processing ownership with `claimToken`
- `Recovery model`: recover expired claims and keep jobs moving after worker loss
- `Cross-process waiting`: supported by Redis and Postgres adapters
- `TypeScript-first`: strict-mode codebase, typed queue payloads, typed adapters

## Quick start

```ts
import { Queue, Worker } from "flexmq";

type EmailJob = { to: string; subject: string };

const queue = new Queue<EmailJob>("emails", { capacity: 1000 });

const worker = new Worker<EmailJob>("emails", {
  concurrency: 2,
  processor: async (job) => {
    console.log(`Sending email to ${job.payload.to}`);
  },
});

async function main() {
  await queue.connect();

  await queue.add({ to: "user@example.com", subject: "Welcome" }, { maxAttempts: 3 });
  await queue.add({ to: "ops@example.com", subject: "Daily report" }, { maxAttempts: 5 });

  await worker.start();
}

main().catch(console.error);
```

## Install

### Core queue

```bash
npm install flexmq
```

### Redis adapter

```bash
npm install flexmq @flexmq/redis ioredis
```

### Postgres adapter

```bash
npm install flexmq @flexmq/postgres pg
```

## Same API, different storage

### In-memory

```ts
import { Queue } from "flexmq";

const queue = new Queue("tasks", { capacity: 1000 });
```

### Redis

```ts
import { Queue } from "flexmq";
import { RedisStorageAdapter } from "@flexmq/redis";

const storage = new RedisStorageAdapter({
  host: "127.0.0.1",
  port: 6379,
  password: process.env.REDIS_PASSWORD,
  capacity: 10000,
});

const queue = new Queue("tasks", { storage });
```

### Postgres

```ts
import { Queue } from "flexmq";
import { PostgresStorageAdapter } from "@flexmq/postgres";

const storage = new PostgresStorageAdapter({
  host: "127.0.0.1",
  port: 5432,
  user: "postgres",
  password: process.env.PGPASSWORD,
  database: "postgres",
  capacity: 10000,
});

await storage.connect();
await storage.ensureSchema();

const queue = new Queue("tasks", { storage });
```

## Reliability model

`flexmq` is built for `at-least-once` delivery.

- workers claim jobs atomically from `pending`
- claimed jobs move to `processing` with `workerId`, `claimedAt`, `leaseUntil`, and `claimToken`
- only the active claim owner can `complete`, `fail`, `retry`, or `renewLease`
- expired claims can be recovered back to `pending`
- processors should be idempotent when duplicate delivery matters

## Backpressure, retries, and waiting

### Backpressure strategies

When the queue reaches capacity, choose the behavior that matches your workload:

- `BackpressureStrategy.BLOCK_PRODUCER`
- `BackpressureStrategy.DROP_OLDEST`
- `BackpressureStrategy.DROP_NEWEST`
- `BackpressureStrategy.ERROR`

### Retries

- set `maxAttempts` per job
- failed jobs retry with exponential delay
- exhausted jobs move to `failed`

### Waiting for completion

`queue.waitForTerminalState(jobId, timeoutMs)` waits until a job becomes `completed` or `failed`.

Redis and Postgres adapters support waiting across processes.

## Why not just pick a single queue product?

Many teams already know whether they want Redis or Postgres. Others do not want to decide too early.

`flexmq` gives you a small, typed queue API now and lets you evolve the storage choice later:

- use memory for fast local development
- use Redis when you want classic queue infrastructure
- use Postgres when your team prefers durable jobs on existing SQL systems

## Packages

- `flexmq` - core queue and worker implementation with in-memory storage
- `@flexmq/redis` - Redis storage adapter for multi-process deployments
- `@flexmq/postgres` - Postgres storage adapter for SQL-backed deployments

## Benchmarks and reliability guides

The repository includes scripts and docs for publishing benchmark and reliability results.

- benchmark guide: `docs/BENCHMARKS.md`
- reliability guide: `docs/RELIABILITY.md`

Run them from the repo root:

```bash
npm run bench:memory
npm run bench:redis
npm run stress:recovery
npm run stress:block-producer
```

## Events

Queue events:

- `queue:connected`
- `queue:disconnected`
- `job:added`
- `job:dropped`

Worker events:

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

## Monorepo development

Install dependencies:

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

Useful commands:

```bash
npm run test:coverage
npm run lint
npm run format:check
npm run clean
```

## License

MIT
