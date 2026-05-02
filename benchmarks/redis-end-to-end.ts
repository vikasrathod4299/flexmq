import Redis from "ioredis";
import { Queue, Worker } from "../packages/core/src";
import { RedisStorageAdapter } from "../packages/redis/src";
import {
  environmentInfo,
  latencySummary,
  nowMs,
  parseIntArg,
  parseStringArg,
  printJsonResult,
  throughputPerSec,
} from "../scripts/helpers";

type Payload = { id: number; body: string };

async function main(): Promise<void> {
  const jobs = parseIntArg("jobs", 2000);
  const concurrency = parseIntArg("concurrency", 4);
  const host = parseStringArg("host", process.env.REDIS_HOST || "localhost");
  const port = parseIntArg("port", Number(process.env.REDIS_PORT || 6379));
  const password = parseStringArg("password", process.env.REDIS_PASSWORD || "");
  const queueName = `bench-redis-${Date.now()}`;

  const storage = new RedisStorageAdapter<Payload>({
    host,
    port,
    password: password || undefined,
    capacity: Math.max(jobs + 10, 1000),
  });
  const cleanupClient = new Redis({
    host,
    port,
    password: password || undefined,
    maxRetriesPerRequest: null,
  });
  const queue = new Queue<Payload>(queueName, { storage, capacity: Math.max(jobs + 10, 1000) });
  const completionLatencies: number[] = [];
  const createdAtByJobId = new Map<string, number>();
  let completed = 0;

  const worker = new Worker<Payload>(queueName, {
    storage,
    concurrency,
    processor: async () => undefined,
  });

  worker.on("job:completed", ({ job }) => {
    const createdAt = createdAtByJobId.get(job.id);
    if (createdAt !== undefined) {
      completionLatencies.push(nowMs() - createdAt);
    }
    completed += 1;
  });

  const startedAt = nowMs();

  try {
    await cleanupClient.ping();
    await queue.connect();
    await worker.start();

    for (let i = 0; i < jobs; i += 1) {
      const job = await queue.add({ id: i, body: "x".repeat(128) }, { maxAttempts: 1 });
      createdAtByJobId.set(job.id, nowMs());
    }

    while (completed < jobs) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    const elapsedMs = nowMs() - startedAt;
    printJsonResult({
      scenario: "redis-end-to-end",
      adapter: "redis",
      jobs,
      elapsedMs,
      throughputPerSec: throughputPerSec(jobs, elapsedMs),
      latencyMs: latencySummary(completionLatencies),
      notes: ["Processor is a no-op", "Measures enqueue-to-complete latency on Redis"],
      environment: environmentInfo(),
    });
  } finally {
    await worker.stop();
    const keys = await cleanupClient.keys(`${queueName}:*`);
    if (keys.length > 0) {
      await cleanupClient.del(...keys);
    }
    await cleanupClient.quit();
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
