# @flexmq/redis

Redis storage adapter for [`flexmq`](https://www.npmjs.com/package/flexmq).

It provides persistent queue storage, delayed job scheduling, processing recovery, and safe multi-worker coordination using Redis + Lua scripts.

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
  queueName: "emails",
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
- `queueName`
- optional auth/db/prefix fields (if needed by your Redis setup)

## Runtime behavior

- Jobs are stored in Redis hashes.
- Pending jobs are consumed with blocking pop semantics.
- Delayed jobs are promoted when due.
- Stuck processing jobs can be recovered and retried.
- Payload is serialized/deserialized safely for object payloads.

## Production notes

- Use a dedicated Redis DB/key prefix per environment.
- Run multiple workers for horizontal scaling.
- Monitor retry/failure rates.
- Ensure clocks are reasonably synchronized across worker machines.
- Use graceful shutdown in workers to avoid duplicate processing windows.

## Related packages

- Core queue: [`flexmq`](https://www.npmjs.com/package/flexmq)

## License

MIT
