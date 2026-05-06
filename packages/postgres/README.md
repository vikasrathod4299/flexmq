# @flexmq/postgres

Postgres storage adapter for `flexmq`.

It provides persistent queue storage, lease-based job claiming, delayed retries, expired-lease recovery, and distributed waiting semantics backed by Postgres.

## Install

```bash
npm install flexmq @flexmq/postgres pg
```

`pg` is a peer dependency and must be installed by the application.

## Requirements

- Node.js `>=16`
- PostgreSQL `>=12` recommended
- `pg` `^8`

## Example

```ts
import { Queue, Worker } from "flexmq";
import { PostgresStorageAdapter } from "@flexmq/postgres";

type Payload = { taskId: string };

const storage = new PostgresStorageAdapter<Payload>({
  host: "127.0.0.1",
  port: 5432,
  user: "postgres",
  password: process.env.PGPASSWORD,
  database: "postgres",
  capacity: 1000,
});

await storage.connect();
await storage.ensureSchema();

const queue = new Queue<Payload>("tasks", { storage });
const worker = new Worker<Payload>("tasks", {
  storage,
  concurrency: 4,
  processor: async (job) => {
    console.log("Processing", job.payload.taskId);
  },
});
```

## Schema setup

`ensureSchema()` is part of the v1 public adapter API.

- it is an explicit setup step
- it is not run automatically by `connect()`
- it creates the required `flexmq_jobs` and `flexmq_job_events` tables and indexes
- production teams may prefer to manage the same schema through their own migration system

Typical local/dev setup:

```ts
await storage.connect();
await storage.ensureSchema();
```

Typical production setup:

- create the same schema using migrations
- call `connect()` normally
- skip `ensureSchema()` at runtime if schema is already managed externally

## Config

Supported config fields:

- `connectionString`
- `host`
- `port`
- `user`
- `password`
- `database`
- `ssl`
- `capacity`

`ssl` follows the `pg` client's `PoolConfig["ssl"]` shape.

## Semantics

- delivery guarantee is `at-least-once`
- queue capacity applies to `pending` jobs only
- claims are lease-based and protected by `claimToken`
- `complete()`, `fail()`, `retry()`, and `renewLease()` verify `claimToken`
- expired claims can be recovered back to `pending`
- `waitForCapacity()` and `waitForTerminalState()` work across processes

## Implementation notes

- claim uses row-level locking with `FOR UPDATE SKIP LOCKED`
- Postgres `LISTEN/NOTIFY` is used as a wake hint, not the source of truth
- storage state remains the durable source of truth
- v1 schema bootstrap is explicit via `ensureSchema()`

## Current status

- integration-tested against a real Postgres instance
- lifecycle contract tests validate parity with Redis adapter semantics
