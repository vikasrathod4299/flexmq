import type { Job } from "flexmq";
import { Queue, Worker, clearMemoryStorageRegistry } from "flexmq";

describe("Worker", () => {
  let queue: Queue<{ data: string }>;
  let worker: Worker<{ data: string }>;

  beforeEach(() => {
    clearMemoryStorageRegistry();
  });

  afterEach(async () => {
    if (worker?.isActive()) {
      await worker.stop();
    }
    if (queue) {
      await queue.disconnect();
    }
  });

  describe("processing jobs", () => {
    it("should process jobs from queue", async () => {
      const processedJobs: Job<{ data: string }>[] = [];

      queue = new Queue("test-queue");
      await queue.connect();

      worker = new Worker("test-queue", {
        concurrency: 1,
        processor: async (job) => {
          processedJobs.push(job);
        },
      });

      await queue.add({ data: "job-1" }, { maxAttempts: 3 });
      await queue.add({ data: "job-2" }, { maxAttempts: 3 });

      await worker.start();
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(processedJobs).toHaveLength(2);
      expect(processedJobs[0].payload.data).toBe("job-1");
      expect(processedJobs[1].payload.data).toBe("job-2");
    });

    it("should emit job:completed event on success", async () => {
      const completedHandler = jest.fn();

      queue = new Queue("test-queue");
      await queue.connect();

      worker = new Worker("test-queue", {
        concurrency: 1,
        processor: async () => {},
      });

      worker.on("job:completed", completedHandler);

      await queue.add({ data: "test" }, { maxAttempts: 3 });
      await worker.start();
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(completedHandler).toHaveBeenCalledTimes(1);
      expect(completedHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          job: expect.objectContaining({ payload: { data: "test" } }),
          duration: expect.any(Number),
        })
      );
    });

    it("should mark job as completed after processing", async () => {
      queue = new Queue("test-queue");
      await queue.connect();

      let jobId: string;

      worker = new Worker("test-queue", {
        concurrency: 1,
        processor: async (job) => {
          jobId = job.id;
        },
      });

      await queue.add({ data: "test" }, { maxAttempts: 3 });
      await worker.start();
      await new Promise((resolve) => setTimeout(resolve, 500));

      const job = await queue.getJob(jobId!);
      expect(job?.status).toBe("completed");
    });
  });

  describe("error handling and retries", () => {
    it("should retry failed jobs with exponential backoff", async () => {
      let attempts = 0;

      queue = new Queue("test-queue");
      await queue.connect();

      worker = new Worker("test-queue", {
        concurrency: 1,
        processor: async () => {
          attempts++;
          if (attempts < 3) throw new Error("Temporary failure");
        },
        timeoutMs: 10,
      });

      const retryHandler = jest.fn();
      worker.on("job:retry", retryHandler);

      await queue.add({ data: "test" }, { maxAttempts: 5 });
      await worker.start();

      await new Promise((resolve) => setTimeout(resolve, 10000));

      expect(attempts).toBe(3);
      expect(retryHandler).toHaveBeenCalledTimes(2);
    }, 15000);

    it("should mark job as failed after max attempts", async () => {
      queue = new Queue("test-queue");
      await queue.connect();

      let jobId: string;

      worker = new Worker("test-queue", {
        concurrency: 1,
        processor: async (job) => {
          jobId = job.id;
          throw new Error("Permanent failure");
        },
      });

      const failedHandler = jest.fn();
      worker.on("job:failed", failedHandler);

      await queue.add({ data: "test" }, { maxAttempts: 1 });
      await worker.start();
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(failedHandler).toHaveBeenCalledTimes(1);

      const job = await queue.getJob(jobId!);
      expect(job?.status).toBe("failed");
      expect(job?.error).toBe("Permanent failure");
    });
  });

  describe("concurrency", () => {
    it("should process multiple jobs concurrently", async () => {
      const startTimes: number[] = [];

      queue = new Queue("test-queue");
      await queue.connect();

      worker = new Worker("test-queue", {
        concurrency: 3,
        processor: async () => {
          startTimes.push(Date.now());
          await new Promise((resolve) => setTimeout(resolve, 200));
        },
      });

      await queue.add({ data: "job-1" }, { maxAttempts: 3 });
      await queue.add({ data: "job-2" }, { maxAttempts: 3 });
      await queue.add({ data: "job-3" }, { maxAttempts: 3 });

      await worker.start();
      await new Promise((resolve) => setTimeout(resolve, 500));

      const maxStartDiff = Math.max(...startTimes) - Math.min(...startTimes);
      expect(maxStartDiff).toBeLessThan(100);
    });
  });

  describe("stop", () => {
    it("should stop processing new jobs", async () => {
      let processedCount = 0;

      queue = new Queue("test-queue");
      await queue.connect();

      worker = new Worker("test-queue", {
        concurrency: 1,
        processor: async () => {
          processedCount++;
          await new Promise((resolve) => setTimeout(resolve, 100));
        },
      });

      await queue.add({ data: "job-1" }, { maxAttempts: 3 });
      await queue.add({ data: "job-2" }, { maxAttempts: 3 });
      await queue.add({ data: "job-3" }, { maxAttempts: 3 });

      await worker.start();
      await new Promise((resolve) => setTimeout(resolve, 150));

      await worker.stop();
      const countAfterStop = processedCount;
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(processedCount).toBe(countAfterStop);
    });
  });

  describe("metrics", () => {
    it("should track completed jobs", async () => {
      queue = new Queue("test-queue");
      await queue.connect();

      worker = new Worker("test-queue", {
        concurrency: 1,
        processor: async () => {},
      });

      await queue.add({ data: "job-1" }, { maxAttempts: 3 });
      await queue.add({ data: "job-2" }, { maxAttempts: 3 });

      await worker.start();
      await new Promise((resolve) => setTimeout(resolve, 500));

      const metrics = worker.getMetrics();
      expect(metrics.jobsCompleted).toBe(2);
    });
  });
});
