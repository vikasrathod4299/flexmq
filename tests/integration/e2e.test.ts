import { Queue, Worker, clearMemoryStorageRegistry } from "flexmq";

describe("End-to-End Integration", () => {
  beforeEach(() => {
    clearMemoryStorageRegistry();
  });

  it("should process jobs from producer to consumer", async () => {
    const results: string[] = [];

    const queue = new Queue<{ message: string }>("integration-test");
    await queue.connect();

    const worker = new Worker<{ message: string }>("integration-test", {
      concurrency: 2,
      processor: async (job) => {
        results.push(job.payload.message);
      },
    });

    await queue.add({ message: "Hello" }, { maxAttempts: 3 });
    await queue.add({ message: "World" }, { maxAttempts: 3 });
    await queue.add({ message: "!" }, { maxAttempts: 3 });

    await worker.start();
    await new Promise((resolve) => setTimeout(resolve, 1000));

    expect(results).toHaveLength(3);
    expect(results).toContain("Hello");
    expect(results).toContain("World");
    expect(results).toContain("!");

    await worker.stop();
    await queue.disconnect();
  });

  it("should handle job failures and retries", async () => {
    let attemptCount = 0;

    const queue = new Queue<{ value: number }>("retry-test");
    await queue.connect();

    const worker = new Worker<{ value: number }>("retry-test", {
      concurrency: 1,
      processor: async () => {
        attemptCount++;
        if (attemptCount < 3) throw new Error("Simulated failure");
      },
      delayedJobCheckIntervalMs: 10,
    });

    await queue.add({ value: 42 }, { maxAttempts: 5 });
    await worker.start();

    await new Promise((resolve) => setTimeout(resolve, 8000));

    expect(attemptCount).toBe(3);

    await worker.stop();
    await queue.disconnect();
  }, 15000);

  it("should recover stuck jobs via lease expiration", async () => {
    const queue = new Queue<{ id: number }>("recovery-test");
    await queue.connect();

    const storage = queue.getStorage();

    await queue.add({ id: 1 }, { maxAttempts: 3 });

    // Claim the job with a very short lease so it expires quickly
    const claim = await storage.claim("recovery-test", {
      workerId: "stuck-worker",
      leaseMs: 50, // 50ms lease — will expire almost immediately
      waitTimeoutMs: 0,
    });
    expect(claim).not.toBeNull();

    const processingJobs = await storage.getProcessingJobs("recovery-test");
    expect(processingJobs).toContain(claim!.job.id);

    // Wait for lease to expire
    await new Promise((resolve) => setTimeout(resolve, 100));

    const processedJobs: number[] = [];
    const worker = new Worker<{ id: number }>("recovery-test", {
      concurrency: 1,
      recoveryIntervalMs: 100,
      processor: async (job) => {
        processedJobs.push(job.payload.id);
      },
    });

    await worker.start();
    await new Promise((resolve) => setTimeout(resolve, 1000));

    expect(processedJobs).toContain(1);

    await worker.stop();
    await queue.disconnect();
  });
});
