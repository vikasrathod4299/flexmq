import Redis from "ioredis";
import { BackpressureStrategy, Queue } from "../packages/core/src";
import { RedisStorageAdapter } from "../packages/redis/src";
import {
  environmentInfo,
  nowMs,
  parseIntArg,
  parseStringArg,
  printJsonResult,
  sleep,
} from "../scripts/helpers";

type Payload = { id: number; email: string };

async function main(): Promise<void> {
  const producers = parseIntArg("producers", 20);
  const capacity = parseIntArg("capacity", 3);
  const host = parseStringArg("host", process.env.REDIS_HOST || "localhost");
  const port = parseIntArg("port", Number(process.env.REDIS_PORT || 6379));
  const password = parseStringArg("password", process.env.REDIS_PASSWORD || "");
  const queueName = `stress-block-producer-${Date.now()}`;

  const storageA = new RedisStorageAdapter<Payload>({
    host,
    port,
    password: password || undefined,
    capacity,
  });
  const storageB = new RedisStorageAdapter<Payload>({
    host,
    port,
    password: password || undefined,
    capacity,
  });
  const cleanupClient = new Redis({
    host,
    port,
    password: password || undefined,
    maxRetriesPerRequest: null,
  });

  const queue = new Queue<Payload>(queueName, {
    storage: storageA,
    capacity,
    backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
  });

  const startedAt = nowMs();
  let resolvedAdds = 0;
  let remoteClaims = 0;

  try {
    await cleanupClient.ping();
    await queue.connect();
    await storageB.connect();

    const addPromises = Array.from({ length: producers }, (_, index) =>
      queue.add({ id: index, email: `user-${index}@test.com` }, { maxAttempts: 1 }).then(() => {
        resolvedAdds += 1;
      })
    );

    while (resolvedAdds < capacity) {
      await sleep(10);
    }

    for (let i = 0; i < producers - capacity; i += 1) {
      const claim = await storageB.claim(queueName, {
        workerId: `remote-${i}`,
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });
      if (!claim) {
        break;
      }
      remoteClaims += 1;
      await storageB.complete(queueName, claim.job.id, claim.claimToken);
    }

    await Promise.all(addPromises);

    const elapsedMs = nowMs() - startedAt;
    printJsonResult({
      scenario: "redis-block-producer-progress",
      adapter: "redis",
      durationMs: elapsedMs,
      summary: {
        producers,
        capacity,
        resolvedAdds,
        remoteClaims,
      },
      invariants: [
        {
          name: "all blocked producers eventually resolve",
          passed: resolvedAdds === producers,
          detail: `resolvedAdds=${resolvedAdds} producers=${producers}`,
        },
        {
          name: "remote claims free pending capacity",
          passed: remoteClaims >= producers - capacity,
          detail: `remoteClaims=${remoteClaims} expected>=${producers - capacity}`,
        },
      ],
      environment: environmentInfo(),
    });
  } finally {
    await queue.disconnect();
    await storageB.disconnect();
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
