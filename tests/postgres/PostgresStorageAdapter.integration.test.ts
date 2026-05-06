/**
 * Integration tests for PostgresStorageAdapter against a real Postgres instance.
 *
 * Requires a running Postgres server:
 *   PGHOST=localhost PGPORT=5432 PGPASSWORD=nbcc_secret PGUSER=nbcc_user PGDATABASE=nbcc_db
 */
import { BackpressureStrategy, Queue, type Job } from "flexmq";
import { Client } from "pg";
import { PostgresStorageAdapter } from "@flexmq/postgres";

const POSTGRES_CONFIG = {
  host: process.env.PGHOST || "localhost",
  port: parseInt(process.env.PGPORT || "5432", 10),
  password: process.env.PGPASSWORD || "nbcc_secret",
  user: process.env.PGUSER || "nbcc_user",
  database: process.env.PGDATABASE || "nbcc_db",
  capacity: 100,
};

const QUEUE = "pg-test-integration";

let adapter: PostgresStorageAdapter<{ email: string }>;
let cleanupClient: Client;

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

async function cleanupTestRows() {
  await cleanupClient.query(`DELETE FROM flexmq_job_events WHERE queue_name = $1`, [QUEUE]);
  await cleanupClient.query(`DELETE FROM flexmq_jobs WHERE queue_name = $1`, [QUEUE]);
}

beforeAll(async () => {
  cleanupClient = new Client({
    host: POSTGRES_CONFIG.host,
    port: POSTGRES_CONFIG.port,
    password: POSTGRES_CONFIG.password,
    user: POSTGRES_CONFIG.user,
    database: POSTGRES_CONFIG.database,
  });
  await cleanupClient.connect();

  adapter = new PostgresStorageAdapter(POSTGRES_CONFIG);
  await adapter.connect();
  await adapter.ensureSchema();
});

afterAll(async () => {
  await cleanupTestRows();
  await adapter.disconnect();
  await cleanupClient.end();
});

beforeEach(async () => {
  await cleanupTestRows();
});

describe("PostgresStorageAdapter Integration", () => {
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
    });

    it("should reject enqueue when queue is at capacity", async () => {
      const smallAdapter = new PostgresStorageAdapter<{ email: string }>({
        ...POSTGRES_CONFIG,
        capacity: 2,
      });
      await smallAdapter.connect();
      await smallAdapter.ensureSchema();

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
  });

  describe("claim", () => {
    it("should claim a job non-blocking and move it to processing", async () => {
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
      expect(claim!.claimToken).toBeDefined();
      expect(await adapter.size(QUEUE)).toBe(0);
      expect(await adapter.getProcessingJobs(QUEUE)).toContain("claim-1");
    });

    it("should wait and claim when waitTimeoutMs > 0", async () => {
      const claimPromise = adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 2000,
      });

      const enqueueLater = new Promise<void>((resolve, reject) => {
        setTimeout(() => {
          adapter
            .enqueue(QUEUE, createJob("claim-wait-1"))
            .then(() => resolve())
            .catch(reject);
        }, 100);
      });

      await expect(claimPromise).resolves.toEqual(
        expect.objectContaining({
          job: expect.objectContaining({
            id: "claim-wait-1",
            status: "processing",
          }),
        })
      );

      await expect(enqueueLater).resolves.toBeUndefined();
    });
  });

  describe("complete / fail / retry", () => {
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
    });

    it("should fail a claimed job", async () => {
      await adapter.enqueue(QUEUE, createJob("fail-1"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      const result = await adapter.fail(QUEUE, "fail-1", claim!.claimToken, "boom");
      expect(result).toBe(true);

      const job = await adapter.getJob(QUEUE, "fail-1");
      expect(job!.status).toBe("failed");
      expect(job!.error).toBe("boom");
    });

    it("should retry a claimed job into delayed state", async () => {
      await adapter.enqueue(QUEUE, createJob("retry-1"));
      const claim = await adapter.claim(QUEUE, {
        workerId: "w1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      const result = await adapter.retry(
        QUEUE,
        "retry-1",
        claim!.claimToken,
        Date.now() + 5000,
        "transient"
      );

      expect(result).toBe(true);
      const job = await adapter.getJob(QUEUE, "retry-1");
      expect(job!.status).toBe("delayed");
      expect(job!.error).toBe("transient");
    });
  });

  describe("waiting APIs", () => {
    it("should wake waitForCapacity after another process claims a pending job", async () => {
      const producerStorage = new PostgresStorageAdapter<{ email: string }>(POSTGRES_CONFIG);
      const consumerStorage = new PostgresStorageAdapter<{ email: string }>(POSTGRES_CONFIG);

      await producerStorage.connect();
      await producerStorage.ensureSchema();
      await consumerStorage.connect();
      await consumerStorage.ensureSchema();

      try {
        await producerStorage.enqueue(QUEUE, createJob("cap-wait-1"));

        const waitPromise = producerStorage.waitForCapacity(QUEUE, 3000);

        const remoteClaim = new Promise<void>((resolve, reject) => {
          setTimeout(() => {
            consumerStorage
              .claim(QUEUE, {
                workerId: "remote-worker",
                leaseMs: 30000,
                waitTimeoutMs: 0,
              })
              .then((claim) => {
                expect(claim).not.toBeNull();
                resolve();
              })
              .catch(reject);
          }, 100);
        });

        await expect(waitPromise).resolves.toBe(true);
        await expect(remoteClaim).resolves.toBeUndefined();
      } finally {
        await producerStorage.disconnect();
        await consumerStorage.disconnect();
      }
    });

    it("should return terminal job from waitForTerminalState after another process completes it", async () => {
      const observerStorage = new PostgresStorageAdapter<{ email: string }>(POSTGRES_CONFIG);
      const workerStorage = new PostgresStorageAdapter<{ email: string }>(POSTGRES_CONFIG);

      await observerStorage.connect();
      await observerStorage.ensureSchema();
      await workerStorage.connect();
      await workerStorage.ensureSchema();

      try {
        await observerStorage.enqueue(QUEUE, createJob("term-wait-1"));
        const claim = await workerStorage.claim(QUEUE, {
          workerId: "remote-worker",
          leaseMs: 30000,
          waitTimeoutMs: 0,
        });

        expect(claim).not.toBeNull();

        const waitPromise = observerStorage.waitForTerminalState(QUEUE, "term-wait-1", 3000);

        const completeLater = new Promise<void>((resolve, reject) => {
          setTimeout(() => {
            workerStorage
              .complete(QUEUE, "term-wait-1", claim!.claimToken)
              .then(() => resolve())
              .catch(reject);
          }, 100);
        });

        await expect(waitPromise).resolves.toEqual(
          expect.objectContaining({
            id: "term-wait-1",
            status: "completed",
          })
        );
        await expect(completeLater).resolves.toBeUndefined();
      } finally {
        await observerStorage.disconnect();
        await workerStorage.disconnect();
      }
    });
  });

  describe("BLOCK_PRODUCER", () => {
    it("should unblock a producer when another process claims a pending job", async () => {
      const storageA = new PostgresStorageAdapter<{ email: string }>(POSTGRES_CONFIG);
      const storageB = new PostgresStorageAdapter<{ email: string }>(POSTGRES_CONFIG);

      await storageA.connect();
      await storageA.ensureSchema();
      await storageB.connect();
      await storageB.ensureSchema();

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
});
