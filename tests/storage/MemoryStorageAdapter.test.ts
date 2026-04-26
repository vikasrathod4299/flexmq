import type { Job } from "flexmq";
import { MemoryStorageAdapter } from "flexmq";

type MemoryStorageInternals<T> = {
  pendingQueue: string[];
  delayedJobs: Map<string, number>;
  processingJobs: Map<
    string,
    { workerId: string; claimToken: string; leaseUntil: number; claimedAt: number }
  >;
};

const getStorageInternals = <T>(value: MemoryStorageAdapter<T>): MemoryStorageInternals<T> =>
  value as unknown as MemoryStorageInternals<T>;

describe("MemoryStorageAdapter", () => {
  let storage: MemoryStorageAdapter<{ data: string }>;

  const createJob = (id: string, payload = { data: "test" }): Job<{ data: string }> => ({
    id,
    payload,
    attempts: 0,
    maxAttempts: 3,
    status: "pending",
    nextAttemptAt: null,
    error: null,
  });

  const defaultClaimOptions = {
    workerId: "worker-1",
    leaseMs: 30000,
    waitTimeoutMs: 0,
  };

  beforeEach(async () => {
    storage = new MemoryStorageAdapter<{ data: string }>(10);
    await storage.connect();
  });

  afterEach(async () => {
    await storage.disconnect();
  });

  describe("enqueue", () => {
    it("should enqueue a job successfully", async () => {
      const job = createJob("job-1");
      const result = await storage.enqueue("test-queue", job);

      expect(result).toBe(true);
      expect(await storage.size("test-queue")).toBe(1);
    });

    it("should reject job when queue is full", async () => {
      for (let i = 0; i < 10; i++) {
        await storage.enqueue("test-queue", createJob(`job-${i}`));
      }
      const result = await storage.enqueue("test-queue", createJob("job-overflow"));
      expect(result).toBe(false);
    });

    it("should set createdAt and updatedAt timestamps", async () => {
      const job = createJob("job-1");
      await storage.enqueue("test-queue", job);

      const storedJob = await storage.getJob("test-queue", "job-1");
      expect(storedJob?.createdAt).toBeDefined();
      expect(storedJob?.updatedAt).toBeDefined();
    });

    it("should deliver directly to waiting claimers", async () => {
      const claimPromise = storage.claim("test-queue", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 2000,
      });
      const job = createJob("job-1");

      await storage.enqueue("test-queue", job);

      const result = await claimPromise;
      expect(result).toEqual(
        expect.objectContaining({
          job: expect.objectContaining({ id: "job-1", status: "processing" }),
          claimToken: expect.any(String),
        })
      );
      expect(result!.job.attempts).toBe(1);
      expect(await storage.size("test-queue")).toBe(0);
      expect(await storage.getProcessingJobs("test-queue")).toEqual(["job-1"]);
    });
  });

  describe("claim", () => {
    it("should claim jobs in FIFO order", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      await storage.enqueue("test-queue", createJob("job-2"));
      await storage.enqueue("test-queue", createJob("job-3"));

      const claim1 = await storage.claim("test-queue", defaultClaimOptions);
      const claim2 = await storage.claim("test-queue", defaultClaimOptions);
      const claim3 = await storage.claim("test-queue", defaultClaimOptions);

      expect(claim1?.job.id).toBe("job-1");
      expect(claim2?.job.id).toBe("job-2");
      expect(claim3?.job.id).toBe("job-3");
    });

    it("should mark job as processing and increment attempts", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", defaultClaimOptions);

      expect(claim?.job.status).toBe("processing");
      expect(claim?.job.attempts).toBe(1);
      expect(claim?.job.updatedAt).toBeDefined();
      expect(claim?.job.claimedAt).toBeDefined();
      expect(claim?.job.leaseUntil).toBeDefined();
      expect(claim?.job.workerId).toBe("worker-1");
      expect(claim?.claimToken).toBeDefined();
    });

    it("should return null when queue is empty with waitTimeoutMs=0", async () => {
      const claim = await storage.claim("test-queue", defaultClaimOptions);
      expect(claim).toBeNull();
    });

    it("should block and wait for job when queue is empty", async () => {
      const claimPromise = storage.claim("test-queue", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 2000,
      });
      setTimeout(async () => {
        await storage.enqueue("test-queue", createJob("delayed-job"));
      }, 500);
      const claim = await claimPromise;
      expect(claim?.job.id).toBe("delayed-job");
    });

    it("should timeout and return null if no job arrives", async () => {
      const start = Date.now();
      const claim = await storage.claim("test-queue", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 1000,
      });
      const elapsed = Date.now() - start;

      expect(claim).toBeNull();
      expect(elapsed).toBeGreaterThanOrEqual(900);
    });

    it("should skip missing queued job ids and continue claiming", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      getStorageInternals(storage).pendingQueue.unshift("missing-job");

      const claim = await storage.claim("test-queue", defaultClaimOptions);

      expect(claim?.job.id).toBe("job-1");
    });
  });

  describe("renewLease", () => {
    it("should extend lease for a claimed job", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", {
        workerId: "worker-1",
        leaseMs: 1000, // short initial lease
        waitTimeoutMs: 0,
      });

      const renewed = await storage.renewLease("test-queue", "job-1", claim!.claimToken, 60000);
      expect(renewed).toBe(true);

      const job = await storage.getJob("test-queue", "job-1");
      expect(job?.leaseUntil).toBeGreaterThanOrEqual(claim!.job.leaseUntil!);
      // New lease should be at least ~59 seconds from now (60000ms), while old was ~1 second
      expect(job!.leaseUntil! - Date.now()).toBeGreaterThan(50000);
    });

    it("should reject renewal with wrong claimToken", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      await storage.claim("test-queue", defaultClaimOptions);

      const renewed = await storage.renewLease("test-queue", "job-1", "wrong-token", 60000);
      expect(renewed).toBe(false);
    });

    it("should reject renewal for non-existent job", async () => {
      const renewed = await storage.renewLease("test-queue", "missing", "token", 60000);
      expect(renewed).toBe(false);
    });
  });

  describe("complete", () => {
    it("should mark job as completed with valid claimToken", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", defaultClaimOptions);

      const completed = await storage.complete("test-queue", "job-1", claim!.claimToken);
      expect(completed).toBe(true);

      const job = await storage.getJob("test-queue", "job-1");
      expect(job?.status).toBe("completed");
      expect(job?.workerId).toBeUndefined();
      expect(job?.claimToken).toBeUndefined();
      expect(await storage.getProcessingJobs("test-queue")).toEqual([]);
    });

    it("should reject completion with wrong claimToken", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      await storage.claim("test-queue", defaultClaimOptions);

      const completed = await storage.complete("test-queue", "job-1", "wrong-token");
      expect(completed).toBe(false);
    });

    it("should return false for non-existent job", async () => {
      const completed = await storage.complete("test-queue", "missing", "token");
      expect(completed).toBe(false);
    });
  });

  describe("fail", () => {
    it("should mark job as failed with valid claimToken", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", defaultClaimOptions);

      const failed = await storage.fail("test-queue", "job-1", claim!.claimToken, "boom");
      expect(failed).toBe(true);

      const job = await storage.getJob("test-queue", "job-1");
      expect(job?.status).toBe("failed");
      expect(job?.error).toBe("boom");
      expect(job?.workerId).toBeUndefined();
      expect(await storage.getProcessingJobs("test-queue")).toEqual([]);
    });

    it("should default error to null when not provided", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", defaultClaimOptions);

      await storage.fail("test-queue", "job-1", claim!.claimToken);

      const job = await storage.getJob("test-queue", "job-1");
      expect(job?.status).toBe("failed");
      expect(job?.error).toBeNull();
    });

    it("should reject fail with wrong claimToken", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      await storage.claim("test-queue", defaultClaimOptions);

      const failed = await storage.fail("test-queue", "job-1", "wrong-token");
      expect(failed).toBe(false);
    });
  });

  describe("retry", () => {
    it("should move job to delayed set with valid claimToken", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", defaultClaimOptions);
      const executeAt = Date.now() + 5000;

      const retried = await storage.retry(
        "test-queue",
        "job-1",
        claim!.claimToken,
        executeAt,
        "temp error"
      );
      expect(retried).toBe(true);

      const job = await storage.getJob("test-queue", "job-1");
      expect(job?.status).toBe("delayed");
      expect(job?.error).toBe("temp error");
      expect(job?.nextAttemptAt?.getTime()).toBe(executeAt);
      expect(job?.workerId).toBeUndefined();
      expect(await storage.getProcessingJobs("test-queue")).toEqual([]);
    });

    it("should reject retry with wrong claimToken", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      await storage.claim("test-queue", defaultClaimOptions);

      const retried = await storage.retry("test-queue", "job-1", "wrong-token", Date.now() + 5000);
      expect(retried).toBe(false);
    });
  });

  describe("peek and queue state", () => {
    it("should return null when peeking an empty queue", async () => {
      await expect(storage.peek("test-queue")).resolves.toBeNull();
    });

    it("should return null when queued job data is missing", async () => {
      getStorageInternals(storage).pendingQueue.push("missing-job");

      await expect(storage.peek("test-queue")).resolves.toBeNull();
    });

    it("should report full and empty states", async () => {
      storage = new MemoryStorageAdapter<{ data: string }>(1);
      await storage.connect();

      expect(await storage.isEmpty("test-queue")).toBe(true);

      await storage.enqueue("test-queue", createJob("job-1"));

      expect(await storage.isEmpty("test-queue")).toBe(false);
      expect(await storage.isFull("test-queue")).toBe(true);
    });
  });

  describe("disconnect", () => {
    it("should resolve waiting claimers and clear all state", async () => {
      const claimPromise1 = storage.claim("test-queue", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 5000,
      });

      await storage.enqueue("test-queue", createJob("job-1"));

      const claimPromise2 = storage.claim("test-queue", {
        workerId: "worker-2",
        leaseMs: 30000,
        waitTimeoutMs: 5000,
      });

      await storage.disconnect();

      const result1 = await claimPromise1;
      const result2 = await claimPromise2;

      expect(result1).toEqual(
        expect.objectContaining({
          job: expect.objectContaining({ id: "job-1" }),
        })
      );
      expect(result2).toBeNull();
      expect(storage.getStats()).toEqual({ pending: 0, processing: 0, delayed: 0, total: 0 });
    });
  });

  describe("delayed jobs", () => {
    it("should promote delayed jobs back to the queue via retry + promoteDelayedJobs", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", defaultClaimOptions);

      // Retry with a past executeAt so it's immediately promotable
      await storage.retry(
        "test-queue",
        "job-1",
        claim!.claimToken,
        Date.now() - 10,
        "retry reason"
      );

      const promoted = await storage.promoteDelayedJobs("test-queue");
      const reClaim = await storage.claim("test-queue", defaultClaimOptions);

      expect(promoted).toBe(1);
      expect(reClaim?.job.id).toBe("job-1");
      expect(reClaim?.job.nextAttemptAt).toBeNull();
    });

    it("should promote delayed jobs directly to waiting claimers", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", defaultClaimOptions);
      await storage.retry("test-queue", "job-1", claim!.claimToken, Date.now() - 10);

      const claimPromise = storage.claim("test-queue", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 2000,
      });
      const promoted = await storage.promoteDelayedJobs("test-queue");

      expect(promoted).toBe(1);
      await expect(claimPromise).resolves.toEqual(
        expect.objectContaining({
          job: expect.objectContaining({ id: "job-1", status: "processing" }),
        })
      );
    });

    it("should skip delayed jobs whose payload record is missing", async () => {
      getStorageInternals(storage).delayedJobs.set("ghost-job", Date.now() - 10);

      const promoted = await storage.promoteDelayedJobs("test-queue");

      expect(promoted).toBe(0);
      expect(storage.getStats().delayed).toBe(0);
    });
  });

  describe("recoverExpiredJobs", () => {
    it("should recover jobs with expired leases", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const claim = await storage.claim("test-queue", {
        workerId: "worker-1",
        leaseMs: 100, // very short lease
        waitTimeoutMs: 0,
      });

      expect(claim).not.toBeNull();

      // Wait for lease to expire
      await new Promise((resolve) => setTimeout(resolve, 150));

      const recovered = await storage.recoverExpiredJobs("test-queue", Date.now());
      expect(recovered).toBe(1);

      const reClaim = await storage.claim("test-queue", defaultClaimOptions);
      expect(reClaim?.job.id).toBe("job-1");
      expect(reClaim?.job.workerId).toBe("worker-1");
    });

    it("should not recover jobs with active leases", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      await storage.claim("test-queue", {
        workerId: "worker-1",
        leaseMs: 60000,
        waitTimeoutMs: 0,
      });

      const recovered = await storage.recoverExpiredJobs("test-queue", Date.now());
      expect(recovered).toBe(0);
    });

    it("should clean up processing entries for missing jobs", async () => {
      getStorageInternals(storage).processingJobs.set("ghost-job", {
        workerId: "worker-1",
        claimToken: "token",
        leaseUntil: Date.now() - 5000,
        claimedAt: Date.now() - 10000,
      });

      const recovered = await storage.recoverExpiredJobs("test-queue", Date.now());

      // ghost-job has no job data, so it gets cleaned from processingJobs but not counted as recovered
      expect(recovered).toBe(0);
      expect(await storage.getProcessingJobs("test-queue")).not.toContain("ghost-job");
    });
  });

  describe("stats and processing ids", () => {
    it("should expose stats and processing ids", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      await storage.enqueue("test-queue", createJob("job-2"));

      // Claim job-1 (moves to processing)
      const claim = await storage.claim("test-queue", defaultClaimOptions);

      // Retry job-2 to delayed
      const claim2 = await storage.claim("test-queue", defaultClaimOptions);
      await storage.retry("test-queue", "job-2", claim2!.claimToken, Date.now() + 10000);

      expect(await storage.getProcessingJobs("test-queue")).toEqual(["job-1"]);
      expect(storage.getStats()).toEqual({
        pending: 0,
        processing: 1,
        delayed: 1,
        total: 2,
      });
    });
  });
});
