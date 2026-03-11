import { Queue, BackpressureStrategy, clearMemoryStorageRegistry } from "flexmq";

describe("Queue", () => {
  let queue: Queue<{ email: string }>;

  beforeEach(() => {
    clearMemoryStorageRegistry();
  });

  afterEach(async () => {
    if (queue) {
      await queue.disconnect();
    }
  });

  describe("constructor", () => {
    it("should create queue with default options", () => {
      queue = new Queue("test-queue");
      expect(queue.getName()).toBe("test-queue");
    });

    it("should create queue with custom capacity", async () => {
      queue = new Queue("test-queue", { capacity: 5 });
      await queue.connect();

      // Fill queue
      for (let i = 0; i < 5; i++) {
        await queue.add({ email: `user${i}@test.com` }, { maxAttempts: 3 });
      }

      // Check size
      const size = await queue.getSize();
      expect(size).toBe(5);
    });
  });

  describe("add", () => {
    beforeEach(async () => {
      queue = new Queue("test-queue", { capacity: 10 });
      await queue.connect();
    });

    it("should add job and return job object", async () => {
      const job = await queue.add({ email: "test@example.com" }, { maxAttempts: 3 });

      expect(job.id).toBeDefined();
      expect(job.payload.email).toBe("test@example.com");
      expect(job.status).toBe("pending");
      expect(job.attempts).toBe(0);
    });

    it("should emit job:added event", async () => {
      const addedHandler = jest.fn();
      queue.on("job:added", addedHandler);

      await queue.add({ email: "test@example.com" }, { maxAttempts: 3 });

      expect(addedHandler).toHaveBeenCalledTimes(1);
      expect(addedHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          payload: { email: "test@example.com" },
        })
      );
    });

    it("should set custom maxAttempts", async () => {
      const job = await queue.add({ email: "test@example.com" }, { maxAttempts: 5 });
      expect(job.maxAttempts).toBe(5);
    });
  });

  describe("backpressure strategies", () => {
    it("should throw error with DROP_NEWEST when full", async () => {
      queue = new Queue("test-queue", {
        capacity: 2,
        backpressureStrategy: BackpressureStrategy.DROP_NEWEST,
      });
      await queue.connect();

      await queue.add({ email: "user1@test.com" }, { maxAttempts: 3 });
      await queue.add({ email: "user2@test.com" }, { maxAttempts: 3 });

      await expect(queue.add({ email: "user3@test.com" }, { maxAttempts: 3 })).rejects.toThrow(
        "Queue is full"
      );
    });

    it("should throw error with ERROR strategy when full", async () => {
      queue = new Queue("test-queue", {
        capacity: 2,
        backpressureStrategy: BackpressureStrategy.ERROR,
      });
      await queue.connect();

      await queue.add({ email: "user1@test.com" }, { maxAttempts: 3 });
      await queue.add({ email: "user2@test.com" }, { maxAttempts: 3 });
      await expect(queue.add({ email: "user3@test.com" }, { maxAttempts: 3 })).rejects.toThrow(
        "Queue is full"
      );
    });
  });

  describe("getJob", () => {
    beforeEach(async () => {
      queue = new Queue("test-queue");
      await queue.connect();
    });

    it("should retrieve job by ID", async () => {
      const addedJob = await queue.add({ email: "test@example.com" }, { maxAttempts: 3 });
      const job = await queue.getJob(addedJob.id);

      expect(job?.id).toBe(addedJob.id);
      expect(job?.payload.email).toBe("test@example.com");
    });

    it("should return null for non-existent job", async () => {
      const job = await queue.getJob("non-existent-id");
      expect(job).toBeNull();
    });
  });

  describe("getSize", () => {
    beforeEach(async () => {
      queue = new Queue("test-queue");
      await queue.connect();
    });

    it("should return correct queue size", async () => {
      expect(await queue.getSize()).toBe(0);

      await queue.add({ email: "user1@test.com" }, { maxAttempts: 3 });
      expect(await queue.getSize()).toBe(1);

      await queue.add({ email: "user2@test.com" }, { maxAttempts: 3 });
      expect(await queue.getSize()).toBe(2);
    });
  });
});
