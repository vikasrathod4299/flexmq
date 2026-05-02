import Redis from "ioredis";
import { Queue } from "../packages/core/src";
import { RedisStorageAdapter } from "../packages/redis/src";
import {
  environmentInfo,
  nowMs,
  parseIntArg,
  parseStringArg,
  printJsonResult,
  sleep,
} from "../scripts/helpers";

type Payload = { id: number; body: string };

async function main(): Promise<void> {
  const jobs = parseIntArg("jobs", 200);
  const host = parseStringArg("host", process.env.REDIS_HOST || "localhost");
  const port = parseIntArg("port", Number(process.env.REDIS_PORT || 6379));
  const password = parseStringArg("password", process.env.REDIS_PASSWORD || "");
  const queueName = `stress-recovery-${Date.now()}`;

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
  const queue = new Queue<Payload>(queueName, { storage });

  let claimed = 0;
  let recovered = 0;
  let completed = 0;
  const startedAt = nowMs();

  try {
    await cleanupClient.ping();
    await queue.connect();

    for (let i = 0; i < jobs; i += 1) {
      await queue.add({ id: i, body: "x".repeat(64) }, { maxAttempts: 2 });
    }

    for (let i = 0; i < jobs; i += 1) {
      const claim = await storage.claim(queueName, {
        workerId: `crash-worker-${i}`,
        leaseMs: 25,
        waitTimeoutMs: 0,
      });
      if (claim) {
        claimed += 1;
      }
    }

    await sleep(60);
    recovered = await storage.recoverExpiredJobs(queueName, Date.now());

    for (let i = 0; i < jobs; i += 1) {
      const claim = await storage.claim(queueName, {
        workerId: `recovery-worker-${i}`,
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });
      if (!claim) {
        continue;
      }
      const success = await storage.complete(queueName, claim.job.id, claim.claimToken);
      if (success) {
        completed += 1;
      }
    }

    const elapsedMs = nowMs() - startedAt;
    printJsonResult({
      scenario: "redis-crash-recovery",
      adapter: "redis",
      durationMs: elapsedMs,
      summary: {
        jobs,
        claimed,
        recovered,
        completed,
      },
      invariants: [
        {
          name: "all jobs initially claimed",
          passed: claimed === jobs,
          detail: `claimed=${claimed} jobs=${jobs}`,
        },
        {
          name: "all expired jobs recovered",
          passed: recovered === jobs,
          detail: `recovered=${recovered} jobs=${jobs}`,
        },
        {
          name: "all recovered jobs completed",
          passed: completed === jobs,
          detail: `completed=${completed} jobs=${jobs}`,
        },
      ],
      environment: environmentInfo(),
    });
  } finally {
    await queue.disconnect();
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
