/**
 * Integration tests for RedisStorageAdapter against a real Redis instance.
 *
 * Requires a running Redis server:
 *   REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=nbcc_redis_secret
 *
 * Run with: npx jest tests/redis/RedisStorageAdapter.integration.test.ts
 */
import Redis from "ioredis";
import { BackpressureStrategy, Queue, type Job } from "flexmq";
import { RedisStorageAdapter } from "@flexmq/redis";

const REDIS_CONFIG = {
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT || "6379", 10),
  password: process.env.REDIS_PASSWORD || "nbcc_redis_secret",
  capacity: 100,
};

const QUEUE = "test-integration";

let adapter: RedisStorageAdapter<{ email: string }>;
let cleanupClient: Redis;

const createJob = (
  id: string,
  overrides: Partial<Job<{ email: string }>> = {}
): Job<{ email: string }> => ({
  id,
  payload: { email: `${id}@test.com` },
  attempts: 0,
  maxAttempts: 3,
  status: "pending",
  nextAttemptAt: null,
  error: null,
  ...overrides,
});

/** Flush all keys matching test queue pattern */
async function flushTestKeys() {
  const keys = await cleanupClient.keys(`${QUEUE}:*`);
  if (keys.length > 0) {
    await cleanupClient.del(...keys);
  }
}

beforeAll(async () => {
  cleanupClient = new Redis({
    host: REDIS_CONFIG.host,
    port: REDIS_CONFIG.port,
    password: REDIS_CONFIG.password,
    maxRetriesPerRequest: null,
  });
  await cleanupClient.ping();

  adapter = new RedisStorageAdapter(REDIS_CONFIG);
  await adapter.connect();
});

afterAll(async () => {
  await flushTestKeys();
  await adapter.disconnect();
  await cleanupClient.quit();
});

beforeEach(async () => {
  await flushTestKeys();
});

describe("RedisStorageAdapter Integration", () => {
  // ── Enqueue ────────────────────────────────────────────────────────

  describe("enqueue", () => {
    it("should enqueue a job and make it retrievable", async () => {
      const job = createJob("enq-1");
      const result = await adapter.enqueue(QUEUE, job);

      expect(result).toBe(true);
      expect(await adapter.size(QUEUE)).toBe(1);
      expect(await adapter.isEmpty(QUEUE)).toBe(false);

      const stored = await adapter.getJob(QUEUE, "enq-1");
      expect(stored).not.toBeNull();
      expect(stored!.id).toBe("enq-1");
      expect(stored!.payload).toEqual({ email: "enq-1@test.com" });
      expect(stored!.status).toBe("pending");
      expect(stored!.attempts).toBe(0);
    });

    it("should reject enqueue when queue is at capacity", async () => {
      const smallAdapter = new RedisStorageAdapter<{ email: string }>({
        ...REDIS_CONFIG,
        capacity: 2,
      });
      await smallAdapter.connect();

      try {
        await smallAdapter.enqueue(QUEUE, createJob("cap-1"));
        await smallAdapter.enqueue(QUEUE, createJob("cap-2"));
        const result = await smallAdapter.enqueue(QUEUE, createJob("cap-3"));
        expect(result).toBe(false);
        expect(await smallAdapter.size(QUEUE)).toBe(2);
      } finally {
        await smallAdapter.disconnect();
      }
    });

    it("should peek at the oldest job without removing it", async () => {
      await adapter.enqueue(QUEUE, createJob("peek-1"));
      await adapter.enqueue(QUEUE, createJob("peek-2"));

      const peeked = await adapter.peek(QUEUE);
      expect(peeked).not.toBeNull();
      // LPUSH + RPOP = FIFO, peek uses LINDEX -1 (oldest = rightmost)
      expect(peeked!.id).toBe("peek-1");
      // peek should not remove the job
      expect(await adapter.size(QUEUE)).toBe(2);
    });
  });

  // ── Claim ──────────────────────────────────────────────────────────

  describe("claim", () => {
    it("should claim a job (non-blocking) and transition to processing", async () => {
      await adapter.enqueue(QUEUE, createJob("claim-1"));

      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      expect(claim).not.toBeNull();
      expect(claim!.job.id).toBe("claim-1");
      expect(claim!.job.status).toBe("processing");
      expect(claim!.job.attempts).toBe(1);
      expect(claim!.job.workerId).toBe("w1");
      expect(claim!.claimToken).toBeDefined();
      expect(typeof claim!.claimToken).toBe("string");

      // Queue should be empty, processing set should have the job
      expect(await adapter.size(QUEUE)).toBe(0);
      const processing = await adapter.getProcessingJobs(QUEUE);
      expect(processing).toContain("claim-1");
    });

    it("should claim a job (blocking) and transition to processing", async () => {
      await adapter.enqueue(QUEUE, createJob("claim-b1"));

      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 1000,
      });

      expect(claim).not.toBeNull();
      expect(claim!.job.id).toBe("claim-b1");
      expect(claim!.job.status).toBe("processing");
      expect(claim!.job.attempts).toBe(1);
    });

    it("should return null when no jobs available (non-blocking)", async () => {
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });
      expect(claim).toBeNull();
    });

    it("should return null when BRPOP times out (blocking)", async () => {
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 1000,
      });
      expect(claim).toBeNull();
    }, 10000);

    it("should claim jobs in FIFO order", async () => {
      await adapter.enqueue(QUEUE, createJob("fifo-1"));
      await adapter.enqueue(QUEUE, createJob("fifo-2"));
      await adapter.enqueue(QUEUE, createJob("fifo-3"));

      const c1 = await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
      const c2 = await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
      const c3 = await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

      expect(c1!.job.id).toBe("fifo-1");
      expect(c2!.job.id).toBe("fifo-2");
      expect(c3!.job.id).toBe("fifo-3");
    });

    it("should set leaseUntil correctly", async () => {
      await adapter.enqueue(QUEUE, createJob("lease-1"));
      const before = Date.now();

      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 60000,
        waitTimeoutMs: 0,
      });

      const after = Date.now();
      expect(claim!.job.leaseUntil).toBeGreaterThanOrEqual(before + 60000);
      expect(claim!.job.leaseUntil).toBeLessThanOrEqual(after + 60000);
    });
  });

  // ── Complete ───────────────────────────────────────────────────────

  describe("complete", () => {
    it("should complete a claimed job", async () => {
      await adapter.enqueue(QUEUE, createJob("comp-1"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      const result = await adapter.complete(QUEUE, "comp-1", claim!.claimToken);
      expect(result).toBe(true);

      const job = await adapter.getJob(QUEUE, "comp-1");
      expect(job!.status).toBe("completed");

      const processing = await adapter.getProcessingJobs(QUEUE);
      expect(processing).not.toContain("comp-1");
    });

    it("should reject complete with wrong claim token", async () => {
      await adapter.enqueue(QUEUE, createJob("comp-2"));
      await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

      const result = await adapter.complete(QUEUE, "comp-2", "wrong-token");
      expect(result).toBe(false);

      // Job should still be processing
      const job = await adapter.getJob(QUEUE, "comp-2");
      expect(job!.status).toBe("processing");
    });
  });

  // ── Fail ───────────────────────────────────────────────────────────

  describe("fail", () => {
    it("should fail a claimed job with error message", async () => {
      await adapter.enqueue(QUEUE, createJob("fail-1"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      const result = await adapter.fail(QUEUE, "fail-1", claim!.claimToken, "something broke");
      expect(result).toBe(true);

      const job = await adapter.getJob(QUEUE, "fail-1");
      expect(job!.status).toBe("failed");
      expect(job!.error).toBe("something broke");

      const processing = await adapter.getProcessingJobs(QUEUE);
      expect(processing).not.toContain("fail-1");
    });

    it("should reject fail with wrong claim token", async () => {
      await adapter.enqueue(QUEUE, createJob("fail-2"));
      await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

      const result = await adapter.fail(QUEUE, "fail-2", "wrong-token", "error");
      expect(result).toBe(false);
    });
  });

  // ── Retry ──────────────────────────────────────────────────────────

  describe("retry", () => {
    it("should move a claimed job to delayed set for retry", async () => {
      await adapter.enqueue(QUEUE, createJob("retry-1"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });
      const executeAt = Date.now() + 5000;

      const result = await adapter.retry(
        QUEUE,
        "retry-1",
        claim!.claimToken,
        executeAt,
        "transient error"
      );
      expect(result).toBe(true);

      const job = await adapter.getJob(QUEUE, "retry-1");
      expect(job!.status).toBe("delayed");
      expect(job!.error).toBe("transient error");

      const processing = await adapter.getProcessingJobs(QUEUE);
      expect(processing).not.toContain("retry-1");
    });

    it("should reject retry with wrong claim token", async () => {
      await adapter.enqueue(QUEUE, createJob("retry-2"));
      await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

      const result = await adapter.retry(QUEUE, "retry-2", "wrong-token", Date.now() + 5000);
      expect(result).toBe(false);
    });
  });

  // ── Renew Lease ────────────────────────────────────────────────────

  describe("renewLease", () => {
    it("should extend the lease on a claimed job", async () => {
      await adapter.enqueue(QUEUE, createJob("renew-1"));
      const claim = await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 5000, waitTimeoutMs: 0 });
      const originalLease = claim!.job.leaseUntil;

      // Renew with a longer lease
      const before = Date.now();
      const result = await adapter.renewLease(QUEUE, "renew-1", claim!.claimToken, 60000);
      const after = Date.now();

      expect(result).toBe(true);

      const job = await adapter.getJob(QUEUE, "renew-1");
      expect(job!.leaseUntil).toBeGreaterThanOrEqual(before + 60000);
      expect(job!.leaseUntil).toBeLessThanOrEqual(after + 60000);
      expect(job!.leaseUntil!).toBeGreaterThan(originalLease!);
    });

    it("should reject renewLease with wrong claim token", async () => {
      await adapter.enqueue(QUEUE, createJob("renew-2"));
      await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

      const result = await adapter.renewLease(QUEUE, "renew-2", "wrong-token", 60000);
      expect(result).toBe(false);
    });
  });

  // ── Promote Delayed Jobs ──────────────────────────────────────────

  describe("promoteDelayedJobs", () => {
    it("should promote delayed jobs whose executeAt has passed", async () => {
      await adapter.enqueue(QUEUE, createJob("delayed-1"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      // Retry with executeAt in the past
      const pastTime = Date.now() - 1000;
      await adapter.retry(QUEUE, "delayed-1", claim!.claimToken, pastTime);

      expect(await adapter.size(QUEUE)).toBe(0);

      const promoted = await adapter.promoteDelayedJobs(QUEUE);
      expect(promoted).toBe(1);

      expect(await adapter.size(QUEUE)).toBe(1);

      const job = await adapter.getJob(QUEUE, "delayed-1");
      expect(job!.status).toBe("pending");
    });

    it("should not promote jobs whose executeAt is in the future", async () => {
      await adapter.enqueue(QUEUE, createJob("delayed-2"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      const futureTime = Date.now() + 60000;
      await adapter.retry(QUEUE, "delayed-2", claim!.claimToken, futureTime);

      const promoted = await adapter.promoteDelayedJobs(QUEUE);
      expect(promoted).toBe(0);
    });
  });

  // ── Recover Expired Jobs ──────────────────────────────────────────

  describe("recoverExpiredJobs", () => {
    it("should recover jobs with expired leases back to pending", async () => {
      await adapter.enqueue(QUEUE, createJob("expired-1"));

      // Claim with a very short lease (1ms effective)
      const claim = await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 1, waitTimeoutMs: 0 });
      expect(claim).not.toBeNull();

      // Wait a tiny bit to ensure lease is expired
      await new Promise((r) => setTimeout(r, 50));

      const recovered = await adapter.recoverExpiredJobs(QUEUE, Date.now());
      expect(recovered).toBe(1);

      // Job should be back in pending
      expect(await adapter.size(QUEUE)).toBe(1);
      const job = await adapter.getJob(QUEUE, "expired-1");
      expect(job!.status).toBe("pending");

      const processing = await adapter.getProcessingJobs(QUEUE);
      expect(processing).not.toContain("expired-1");
    });

    it("should not recover jobs with active leases", async () => {
      await adapter.enqueue(QUEUE, createJob("active-1"));
      await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 60000, waitTimeoutMs: 0 });

      const recovered = await adapter.recoverExpiredJobs(QUEUE, Date.now());
      expect(recovered).toBe(0);
    });
  });

  // ── Full lifecycle ─────────────────────────────────────────────────

  describe("full lifecycle", () => {
    it("enqueue -> claim -> complete", async () => {
      await adapter.enqueue(QUEUE, createJob("life-1"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });
      await adapter.complete(QUEUE, "life-1", claim!.claimToken);

      const job = await adapter.getJob(QUEUE, "life-1");
      expect(job!.status).toBe("completed");
      expect(await adapter.size(QUEUE)).toBe(0);
      expect(await adapter.getProcessingJobs(QUEUE)).toEqual([]);
    });

    it("enqueue -> claim -> retry -> promote -> claim -> complete", async () => {
      await adapter.enqueue(QUEUE, createJob("life-2"));

      // First attempt: claim and retry
      const c1 = await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
      await adapter.retry(QUEUE, "life-2", c1!.claimToken, Date.now() - 1);

      // Promote the delayed job
      await adapter.promoteDelayedJobs(QUEUE);

      // Second attempt: claim and complete
      const c2 = await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
      expect(c2!.job.attempts).toBe(2);
      await adapter.complete(QUEUE, "life-2", c2!.claimToken);

      const job = await adapter.getJob(QUEUE, "life-2");
      expect(job!.status).toBe("completed");
    });

    it("enqueue -> claim -> fail (terminal)", async () => {
      await adapter.enqueue(QUEUE, createJob("life-3"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });
      await adapter.fail(QUEUE, "life-3", claim!.claimToken, "fatal error");

      const job = await adapter.getJob(QUEUE, "life-3");
      expect(job!.status).toBe("failed");
      expect(job!.error).toBe("fatal error");
    });

    it("enqueue -> claim -> lease expires -> recover -> claim again", async () => {
      await adapter.enqueue(QUEUE, createJob("life-4"));

      const c1 = await adapter.claim(QUEUE, { workerId: "w1", leaseMs: 1, waitTimeoutMs: 0 });
      expect(c1).not.toBeNull();

      await new Promise((r) => setTimeout(r, 50));

      const recovered = await adapter.recoverExpiredJobs(QUEUE, Date.now());
      expect(recovered).toBe(1);

      // Should be claimable again
      const c2 = await adapter.claim(QUEUE, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
      expect(c2).not.toBeNull();
      expect(c2!.job.id).toBe("life-4");
      expect(c2!.job.attempts).toBe(2);
      expect(c2!.job.workerId).toBe("w2");
    });

    it("concurrent workers cannot operate on same job with different tokens", async () => {
      await adapter.enqueue(QUEUE, createJob("life-5"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      // Stale worker tries to complete with wrong token
      const staleComplete = await adapter.complete(QUEUE, "life-5", "stale-token");
      expect(staleComplete).toBe(false);

      const staleFail = await adapter.fail(QUEUE, "life-5", "stale-token", "oops");
      expect(staleFail).toBe(false);

      const staleRenew = await adapter.renewLease(QUEUE, "life-5", "stale-token", 60000);
      expect(staleRenew).toBe(false);

      const staleRetry = await adapter.retry(QUEUE, "life-5", "stale-token", Date.now() + 5000);
      expect(staleRetry).toBe(false);

      // Real owner can still complete
      const realComplete = await adapter.complete(QUEUE, "life-5", claim!.claimToken);
      expect(realComplete).toBe(true);
    });
  });

  describe("BLOCK_PRODUCER distributed wake-up", () => {
    it("should unblock a producer when another process claims a pending job", async () => {
      const storageA = new RedisStorageAdapter<{ email: string }>(REDIS_CONFIG);
      const storageB = new RedisStorageAdapter<{ email: string }>(REDIS_CONFIG);

      await storageA.connect();
      await storageB.connect();

      const producerQueue = new Queue<{ email: string }>(QUEUE, {
        storage: storageA,
        capacity: 1,
        backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
      });

      try {
        await producerQueue.connect();

        await producerQueue.add({ email: "first@test.com" }, { maxAttempts: 3 });
        const blockedAdd = producerQueue.add({ email: "second@test.com" }, { maxAttempts: 5 });

        await Promise.resolve();

        const claimed = await storageB.claim(QUEUE, {
          workerId: "remote-worker",
          leaseMs: 30000,
          waitTimeoutMs: 0,
        });

        expect(claimed).not.toBeNull();
        expect(claimed!.job.payload.email).toBe("first@test.com");

        await expect(blockedAdd).resolves.toEqual(
          expect.objectContaining({
            payload: { email: "second@test.com" },
            maxAttempts: 5,
          })
        );
      } finally {
        await producerQueue.disconnect();
        await storageB.disconnect();
      }
    });
  });

  describe("waitForTerminalState", () => {
    it("should return completed job after another process completes it", async () => {
      const storageA = new RedisStorageAdapter<{ email: string }>(REDIS_CONFIG);
      const storageB = new RedisStorageAdapter<{ email: string }>(REDIS_CONFIG);

      await storageA.connect();
      await storageB.connect();

      const queue = new Queue<{ email: string }>(QUEUE, { storage: storageA, capacity: 10 });

      try {
        await queue.connect();

        const job = await queue.add({ email: "terminal@test.com" }, { maxAttempts: 3 });
        const claim = await storageB.claim(QUEUE, {
          workerId: "remote-worker",
          leaseMs: 30000,
          waitTimeoutMs: 0,
        });

        expect(claim).not.toBeNull();

        const waitPromise = queue.waitForTerminalState(job.id, 5000);

        setTimeout(async () => {
          await storageB.complete(QUEUE, job.id, claim!.claimToken);
        }, 50);

        await expect(waitPromise).resolves.toEqual(
          expect.objectContaining({
            id: job.id,
            status: "completed",
          })
        );
      } finally {
        await queue.disconnect();
        await storageB.disconnect();
      }
    });

    it("should return null when the job does not reach a terminal state before timeout", async () => {
      const storage = new RedisStorageAdapter<{ email: string }>(REDIS_CONFIG);
      await storage.connect();

      const queue = new Queue<{ email: string }>(QUEUE, { storage, capacity: 10 });

      try {
        await queue.connect();
        const job = await queue.add({ email: "pending@test.com" }, { maxAttempts: 3 });

        await expect(queue.waitForTerminalState(job.id, 100)).resolves.toBeNull();
      } finally {
        await queue.disconnect();
      }
    });
  });
});
