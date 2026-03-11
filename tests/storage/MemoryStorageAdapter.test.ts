import type { Job } from "flexmq";
import { MemoryStorageAdapter } from "flexmq";

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

        it("should deliver directly to waiting consumers", async () => {
            const dequeuePromise = storage.dequeue("test-queue", 2);
            const job = createJob("job-1");

            await storage.enqueue("test-queue", job);

            await expect(dequeuePromise).resolves.toEqual(
                expect.objectContaining({ id: "job-1", status: "processing" }),
            );
            expect(job.attempts).toBe(1);
            expect(await storage.size("test-queue")).toBe(0);
            expect(await storage.getProcessingJobs("test-queue")).toEqual(["job-1"]);
        });
  });

  describe("dequeue", () => {
    it("should dequeue jobs in FIFO order", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      await storage.enqueue("test-queue", createJob("job-2"));
      await storage.enqueue("test-queue", createJob("job-3"));

      const job1 = await storage.dequeue("test-queue", 0);
      const job2 = await storage.dequeue("test-queue", 0);
      const job3 = await storage.dequeue("test-queue", 0);

      expect(job1?.id).toBe("job-1");
      expect(job2?.id).toBe("job-2");
      expect(job3?.id).toBe("job-3");
    });

    it("should mark job as processing and increment attempts", async () => {
      await storage.enqueue("test-queue", createJob("job-1"));
      const job = await storage.dequeue("test-queue", 0);

      expect(job?.status).toBe("processing");
      expect(job?.attempts).toBe(1);
      expect(job?.updatedAt).toBeDefined();
      expect(job?.processingStartedAt).toBeDefined();
    });

    it("should return null when queue is empty with timeout=0", async () => {
      const job = await storage.dequeue("test-queue", 0);
      expect(job).toBeNull();
    });

    it("should block and wait for job when queue is empty", async () => {
      const dequeuePromise = storage.dequeue("test-queue", 2);
      setTimeout(async () => {
        await storage.enqueue("test-queue", createJob("delayed-job"));
      }, 500);
      const job = await dequeuePromise;
      expect(job?.id).toBe("delayed-job");
    });

    it("should timeout and return null if no job arrives", async () => {
      const start = Date.now();
      const job = await storage.dequeue("test-queue", 1);
      const elapsed = Date.now() - start;

      expect(job).toBeNull();
      expect(elapsed).toBeGreaterThanOrEqual(900);
    });

        it("should skip missing queued job ids and continue dequeueing", async () => {
            await storage.enqueue("test-queue", createJob("job-1"));
            (storage as any).queue.unshift("missing-job");

            const job = await storage.dequeue("test-queue", 0);

            expect(job?.id).toBe("job-1");
        });
    });

    describe("peek and queue state", () => {
        it("should return null when peeking an empty queue", async () => {
            await expect(storage.peek("test-queue")).resolves.toBeNull();
        });

        it("should return null when queued job data is missing", async () => {
            (storage as any).queue.push("missing-job");

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
        it("should resolve waiting consumers and clear all state", async () => {
            const waitingConsumer = storage.dequeue("test-queue", 5);

            await storage.enqueue("test-queue", createJob("job-1"));
            await storage.scheduleDelayed("test-queue", createJob("job-2"), Date.now() + 1000);

            const secondWaitingConsumer = storage.dequeue("test-queue", 5);
            await storage.disconnect();

            await expect(waitingConsumer).resolves.toEqual(expect.objectContaining({ id: "job-1" }));
            await expect(secondWaitingConsumer).resolves.toBeNull();
            expect(storage.getStats()).toEqual({ pending: 0, processing: 0, delayed: 0, total: 0 });
        });
    });

    describe("delayed jobs", () => {
        it("should schedule and promote delayed jobs back to the queue", async () => {
            const job = createJob("job-1");
            await storage.scheduleDelayed("test-queue", job, Date.now() - 10);

            const promoted = await storage.promoteDelayedJobs("test-queue");
            const queuedJob = await storage.dequeue("test-queue", 0);

            expect(promoted).toBe(1);
            expect(queuedJob?.id).toBe("job-1");
            expect(queuedJob?.nextAttemptAt).toBeNull();
        });

        it("should promote delayed jobs directly to waiting consumers", async () => {
            const job = createJob("job-1");
            await storage.scheduleDelayed("test-queue", job, Date.now() - 10);

            const dequeuePromise = storage.dequeue("test-queue", 2);
            const promoted = await storage.promoteDelayedJobs("test-queue");

            expect(promoted).toBe(1);
            await expect(dequeuePromise).resolves.toEqual(
                expect.objectContaining({ id: "job-1", status: "processing" }),
            );
        });

        it("should skip delayed jobs whose payload record is missing", async () => {
            (storage as any).delayedJobs.set("ghost-job", Date.now() - 10);

            const promoted = await storage.promoteDelayedJobs("test-queue");

            expect(promoted).toBe(0);
            expect(storage.getStats().delayed).toBe(0);
        });
    });

    describe("job lifecycle updates", () => {
        it("should mark processing metadata only when the job exists", async () => {
            await storage.markProcessing("test-queue", "missing-job", "worker-1");

            const job = createJob("job-1");
            await storage.enqueue("test-queue", job);
            await storage.markProcessing("test-queue", "job-1", "worker-1");

            const storedJob = await storage.getJob("test-queue", "job-1");
            expect(storedJob?.status).toBe("processing");
            expect(storedJob?.workerId).toBe("worker-1");
            expect(await storage.getProcessingJobs("test-queue")).toContain("job-1");
        });

        it("should mark jobs completed and ignore unknown ids", async () => {
            await storage.markCompleted("test-queue", "missing-job");

            await storage.enqueue("test-queue", createJob("job-1"));
            await storage.markProcessing("test-queue", "job-1", "worker-1");
            await storage.markCompleted("test-queue", "job-1");

            const storedJob = await storage.getJob("test-queue", "job-1");
            expect(storedJob?.status).toBe("completed");
            expect(storedJob?.processingStartedAt).toBeUndefined();
        });

        it("should mark jobs failed with optional error handling", async () => {
            await storage.markFailed("test-queue", "missing-job", "ignored");

            await storage.enqueue("test-queue", createJob("job-1"));
            await storage.markProcessing("test-queue", "job-1", "worker-1");
            await storage.markFailed("test-queue", "job-1");

            const storedJob = await storage.getJob("test-queue", "job-1");
            expect(storedJob?.status).toBe("failed");
            expect(storedJob?.error).toBeNull();
        });

        it("should update processing indexes only for processing jobs with timestamps", async () => {
            const processingJob = createJob("job-1");
            processingJob.status = "processing";
            processingJob.processingStartedAt = Date.now() - 1000;

            await storage.updateJob("test-queue", processingJob);
            expect(await storage.getProcessingJobs("test-queue")).toContain("job-1");

            const completedJob = createJob("job-2");
            completedJob.status = "completed";
            await storage.updateJob("test-queue", completedJob);

            expect(await storage.getProcessingJobs("test-queue")).not.toContain("job-2");
            expect((await storage.getJob("test-queue", "job-2"))?.updatedAt).toBeDefined();
        });
    });

    describe("recovery and stats", () => {
        it("should recover stuck jobs and clean up missing processing entries", async () => {
            const stuckJob = createJob("job-1");
            await storage.enqueue("test-queue", stuckJob);
            await storage.markProcessing("test-queue", "job-1", "worker-1");
            (storage as any).processingJobs.set("ghost-job", Date.now() - 5000);
            (storage as any).processingJobs.set("job-1", Date.now() - 5000);

            const recovered = await storage.recoverStuckJobs("test-queue", 1000);
            const recoveredJob = await storage.dequeue("test-queue", 0);

            expect(recovered).toBe(1);
            expect(recoveredJob?.id).toBe("job-1");
            expect(recoveredJob?.workerId).toBeUndefined();
            expect(await storage.getProcessingJobs("test-queue")).not.toContain("ghost-job");
        });

        it("should expose stats and processing ids", async () => {
            await storage.enqueue("test-queue", createJob("job-1"));
            await storage.enqueue("test-queue", createJob("job-2"));
            await storage.scheduleDelayed("test-queue", createJob("job-3"), Date.now() + 1000);
            await storage.markProcessing("test-queue", "job-1", "worker-1");

            expect(await storage.getProcessingJobs("test-queue")).toEqual(["job-1"]);
            expect(storage.getStats()).toEqual({
                pending: 2,
                processing: 1,
                delayed: 1,
                total: 3,
            });
        });
  });
});
