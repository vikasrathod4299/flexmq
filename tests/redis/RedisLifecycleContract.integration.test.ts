/**
 * Lifecycle Contract Proof Tests for RedisStorageAdapter
 *
 * These tests prove that the Redis adapter upholds every guarantee
 * defined in JOB_LIFECYCLE_CONTRACT.md. Assertions are made against
 * raw Redis data structures (Option B) so the adapter cannot mask
 * its own bugs.
 *
 * Requires a running Redis server:
 *   REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=nbcc_redis_secret
 */
import Redis from "ioredis";
import { type Job } from "flexmq";
import { RedisStorageAdapter } from "@flexmq/redis";

const REDIS_CONFIG = {
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT || "6379", 10),
  password: process.env.REDIS_PASSWORD || "nbcc_redis_secret",
  capacity: 100,
};

const Q = "contract-test";

// Redis key helpers (must match adapter internals)
const keys = {
  pending: `${Q}:pending`,
  processing: `${Q}:processing`,
  delayed: `${Q}:delayed`,
  job: (id: string) => `${Q}:job:${id}`,
};

let adapter: RedisStorageAdapter<{ email: string }>;
let redis: Redis;

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

async function flushTestKeys() {
  const testKeys = await redis.keys(`${Q}:*`);
  if (testKeys.length > 0) await redis.del(...testKeys);
}

beforeAll(async () => {
  redis = new Redis({
    host: REDIS_CONFIG.host,
    port: REDIS_CONFIG.port,
    password: REDIS_CONFIG.password,
    maxRetriesPerRequest: null,
  });
  await redis.ping();

  adapter = new RedisStorageAdapter(REDIS_CONFIG);
  await adapter.connect();
});

afterAll(async () => {
  await flushTestKeys();
  await adapter.disconnect();
  await redis.quit();
});

beforeEach(async () => {
  await flushTestKeys();
});

describe("Lifecycle Contract Proofs (JOB_LIFECYCLE_CONTRACT.md)", () => {
  // ─────────────────────────────────────────────────────────────────
  // Contract §Claim semantics: "only one worker may successfully
  // claim a given job at a time" + "claim must be atomic"
  // ─────────────────────────────────────────────────────────────────
  it("1. Concurrent claim race — exactly one worker wins", async () => {
    await adapter.enqueue(Q, createJob("race-1"));

    // 10 workers race to claim the same job (non-blocking)
    const claims = await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        adapter.claim(Q, { workerId: `w${i}`, leaseMs: 30000, waitTimeoutMs: 0 })
      )
    );

    const winners = claims.filter((c) => c !== null);
    expect(winners).toHaveLength(1);

    // Redis ground truth: pending list empty, processing has exactly one member
    expect(await redis.llen(keys.pending)).toBe(0);
    const processingMembers = await redis.zrange(keys.processing, 0, -1);
    expect(processingMembers).toEqual(["race-1"]);
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Claim semantics: "the job becomes invisible to other
  // workers until lease expiry"
  // ─────────────────────────────────────────────────────────────────
  it("2. Claimed job is invisible — second claim gets nothing", async () => {
    await adapter.enqueue(Q, createJob("invis-1"));
    await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    const second = await adapter.claim(Q, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
    expect(second).toBeNull();

    // Ground truth: pending list is empty
    expect(await redis.llen(keys.pending)).toBe(0);
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Ack and completion semantics: "A job is considered
  // acknowledged only when storage durably records
  // processing -> completed"
  // ─────────────────────────────────────────────────────────────────
  it("3. Complete is the ack boundary — status transitions durably", async () => {
    await adapter.enqueue(Q, createJob("ack-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    // Before complete: still processing in Redis
    expect(await redis.hget(keys.job("ack-1"), "status")).toBe("processing");
    expect(await redis.zscore(keys.processing, "ack-1")).not.toBeNull();

    await adapter.complete(Q, "ack-1", claim!.claimToken);

    // After complete: status is completed, removed from processing set
    expect(await redis.hget(keys.job("ack-1"), "status")).toBe("completed");
    expect(await redis.zscore(keys.processing, "ack-1")).toBeNull();
    // Claim metadata cleared
    expect(await redis.hget(keys.job("ack-1"), "claimToken")).toBe("");
    expect(await redis.hget(keys.job("ack-1"), "workerId")).toBe("");
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Crash recovery semantics: "If a worker dies after
  // claiming a job but before writing a terminal state… once the
  // lease has expired, the job is considered stale… a recovery
  // process may move the job back to pending"
  // ─────────────────────────────────────────────────────────────────
  it("4. Crash-before-ack does not lose jobs — recovered after lease expiry", async () => {
    await adapter.enqueue(Q, createJob("crash-1"));

    // Worker claims with 1ms lease then "crashes" (never calls complete/fail)
    await adapter.claim(Q, { workerId: "w1", leaseMs: 1, waitTimeoutMs: 0 });
    await new Promise((r) => setTimeout(r, 50));

    // Before recovery: job stuck in processing
    expect(await redis.hget(keys.job("crash-1"), "status")).toBe("processing");
    expect(await redis.zscore(keys.processing, "crash-1")).not.toBeNull();

    await adapter.recoverExpiredJobs(Q, Date.now());

    // After recovery: job back in pending list, not in processing set
    expect(await redis.hget(keys.job("crash-1"), "status")).toBe("pending");
    expect(await redis.zscore(keys.processing, "crash-1")).toBeNull();
    expect(await redis.llen(keys.pending)).toBe(1);
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Delivery goal: "at-least-once delivery… a job may be
  // delivered more than once in crash or retry scenarios"
  // ─────────────────────────────────────────────────────────────────
  it("5. At-least-once delivery — job delivered twice after crash recovery", async () => {
    await adapter.enqueue(Q, createJob("atleast-1"));

    // First delivery
    const c1 = await adapter.claim(Q, { workerId: "w1", leaseMs: 1, waitTimeoutMs: 0 });
    expect(c1).not.toBeNull();
    expect(parseInt((await redis.hget(keys.job("atleast-1"), "attempts")) as string)).toBe(1);

    // Worker crashes — lease expires
    await new Promise((r) => setTimeout(r, 50));
    await adapter.recoverExpiredJobs(Q, Date.now());

    // Second delivery
    const c2 = await adapter.claim(Q, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
    expect(c2).not.toBeNull();
    expect(parseInt((await redis.hget(keys.job("atleast-1"), "attempts")) as string)).toBe(2);

    // Different worker, different token
    expect(c2!.job.workerId).toBe("w2");
    expect(c2!.claimToken).not.toBe(c1!.claimToken);
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Failure and retry semantics: "retry scheduling must
  // remove the job from processing… a retried job must not still
  // appear as actively claimed"
  // Contract §State model: "a job must never be in both processing
  // and delayed"
  // ─────────────────────────────────────────────────────────────────
  it("6. Retry leaves processing cleanly — job in delayed, not processing", async () => {
    await adapter.enqueue(Q, createJob("retry-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
    const executeAt = Date.now() + 60000;

    await adapter.retry(Q, "retry-1", claim!.claimToken, executeAt, "transient");

    // Ground truth: NOT in processing sorted set
    expect(await redis.zscore(keys.processing, "retry-1")).toBeNull();

    // Ground truth: IS in delayed sorted set with correct score
    const delayedScore = await redis.zscore(keys.delayed, "retry-1");
    expect(delayedScore).not.toBeNull();
    expect(parseInt(delayedScore as string)).toBe(executeAt);

    // Job hash: status=delayed, claim metadata cleared
    expect(await redis.hget(keys.job("retry-1"), "status")).toBe("delayed");
    expect(await redis.hget(keys.job("retry-1"), "claimToken")).toBe("");
    expect(await redis.hget(keys.job("retry-1"), "workerId")).toBe("");
    expect(await redis.hget(keys.job("retry-1"), "leaseUntil")).toBe("");
    expect(await redis.hget(keys.job("retry-1"), "error")).toBe("transient");
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Delayed job semantics: "a delayed job remains in
  // delayed until nextAttemptAt <= now… delayed jobs are not visible
  // to normal claim operations while still scheduled for the future"
  // ─────────────────────────────────────────────────────────────────
  it("7. Delayed jobs are not claimable early", async () => {
    await adapter.enqueue(Q, createJob("early-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    // Retry with executeAt far in the future
    await adapter.retry(Q, "early-1", claim!.claimToken, Date.now() + 600000);

    // Promotion should not move it
    const promoted = await adapter.promoteDelayedJobs(Q);
    expect(promoted).toBe(0);

    // Pending list is empty — job cannot be claimed
    expect(await redis.llen(keys.pending)).toBe(0);

    const claimAttempt = await adapter.claim(Q, {
      workerId: "w2",
      leaseMs: 30000,
      waitTimeoutMs: 0,
    });
    expect(claimAttempt).toBeNull();

    // Ground truth: still in delayed set
    expect(await redis.zscore(keys.delayed, "early-1")).not.toBeNull();
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Delayed job semantics: "once promoted, the job becomes
  // eligible for normal claiming"
  // ─────────────────────────────────────────────────────────────────
  it("8. Delayed job promoted at correct time becomes claimable", async () => {
    await adapter.enqueue(Q, createJob("promo-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    // Retry with executeAt in the past
    await adapter.retry(Q, "promo-1", claim!.claimToken, Date.now() - 1000);

    const promoted = await adapter.promoteDelayedJobs(Q);
    expect(promoted).toBe(1);

    // Ground truth: removed from delayed, present in pending
    expect(await redis.zscore(keys.delayed, "promo-1")).toBeNull();
    expect(await redis.llen(keys.pending)).toBe(1);
    expect(await redis.hget(keys.job("promo-1"), "status")).toBe("pending");

    // Can be claimed again
    const c2 = await adapter.claim(Q, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
    expect(c2).not.toBeNull();
    expect(c2!.job.id).toBe("promo-1");
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Failure and retry semantics: "final failure is a
  // durable terminal state… failed jobs must remain inspectable"
  // ─────────────────────────────────────────────────────────────────
  it("9. Failed jobs are terminal and inspectable in Redis", async () => {
    await adapter.enqueue(Q, createJob("term-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    await adapter.fail(Q, "term-1", claim!.claimToken, "fatal: out of memory");

    // Ground truth: status=failed, error stored, not in processing, not in pending
    expect(await redis.hget(keys.job("term-1"), "status")).toBe("failed");
    expect(await redis.hget(keys.job("term-1"), "error")).toBe("fatal: out of memory");
    expect(await redis.zscore(keys.processing, "term-1")).toBeNull();
    expect(await redis.llen(keys.pending)).toBe(0);

    // Job hash still exists — inspectable
    const allFields = await redis.hgetall(keys.job("term-1"));
    expect(allFields).toHaveProperty("id", "term-1");
    expect(allFields).toHaveProperty("payload");
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Lease and visibility timeout semantics: "the worker
  // that claimed the job owns it until leaseUntil… the owning worker
  // may renew the lease"
  // Contract §Crash recovery semantics: "recovery must not affect
  // jobs with active leases"
  // ─────────────────────────────────────────────────────────────────
  it("10. Stale worker token rejected after recovery + new claim", async () => {
    await adapter.enqueue(Q, createJob("stale-1"));

    // Worker 1 claims with tiny lease
    const c1 = await adapter.claim(Q, { workerId: "w1", leaseMs: 1, waitTimeoutMs: 0 });
    const staleToken = c1!.claimToken;

    await new Promise((r) => setTimeout(r, 50));
    await adapter.recoverExpiredJobs(Q, Date.now());

    // Worker 2 claims the recovered job
    const c2 = await adapter.claim(Q, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
    expect(c2).not.toBeNull();

    // Stale worker 1 tries all operations with old token — all rejected
    expect(await adapter.complete(Q, "stale-1", staleToken)).toBe(false);
    expect(await adapter.fail(Q, "stale-1", staleToken, "oops")).toBe(false);
    expect(await adapter.renewLease(Q, "stale-1", staleToken, 60000)).toBe(false);
    expect(await adapter.retry(Q, "stale-1", staleToken, Date.now() + 5000)).toBe(false);

    // Ground truth: job still processing under worker 2's token
    expect(await redis.hget(keys.job("stale-1"), "status")).toBe("processing");
    expect(await redis.hget(keys.job("stale-1"), "claimToken")).toBe(c2!.claimToken);
    expect(await redis.hget(keys.job("stale-1"), "workerId")).toBe("w2");
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Crash recovery semantics: "recovery must not affect
  // jobs with active leases"
  // ─────────────────────────────────────────────────────────────────
  it("11. Recovery does not touch jobs with active leases", async () => {
    await adapter.enqueue(Q, createJob("active-1"));

    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 60000, waitTimeoutMs: 0 });
    const originalScore = await redis.zscore(keys.processing, "active-1");

    const recovered = await adapter.recoverExpiredJobs(Q, Date.now());
    expect(recovered).toBe(0);

    // Ground truth: still in processing with same score, not in pending
    expect(await redis.zscore(keys.processing, "active-1")).toBe(originalScore);
    expect(await redis.hget(keys.job("active-1"), "status")).toBe("processing");
    expect(await redis.hget(keys.job("active-1"), "claimToken")).toBe(claim!.claimToken);
    expect(await redis.llen(keys.pending)).toBe(0);
  });

  // ─────────────────────────────────────────────────────────────────
  // Contract §Ordering semantics: "FIFO applies to pending jobs
  // that are ready to run"
  // ─────────────────────────────────────────────────────────────────
  it("12. FIFO ordering among pending jobs", async () => {
    await adapter.enqueue(Q, createJob("fifo-1"));
    await adapter.enqueue(Q, createJob("fifo-2"));
    await adapter.enqueue(Q, createJob("fifo-3"));

    // Ground truth: Redis list order (LPUSH = left, RPOP = right → FIFO)
    // lrange 0 -1 returns [newest ... oldest], so rightmost = fifo-1
    const listContents = await redis.lrange(keys.pending, 0, -1);
    expect(listContents).toEqual(["fifo-3", "fifo-2", "fifo-1"]);

    // Claims should come out FIFO
    const c1 = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
    const c2 = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
    const c3 = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    expect(c1!.job.id).toBe("fifo-1");
    expect(c2!.job.id).toBe("fifo-2");
    expect(c3!.job.id).toBe("fifo-3");
  });
});
