import {
  Queue,
  BackpressureStrategy,
  clearMemoryStorageRegistry,
  type StorageAdapter,
  type Job,
  type MemoryStorageAdapter,
} from "flexmq";

type QueuePayload = { email: string };
type QueueJob = Job<QueuePayload>;
type QueueInternals = {
  enqueueLock: boolean;
  dropOldestAndEnqueue: (job: QueueJob) => Promise<QueueJob>;
  drainWaitingProducers: () => Promise<void>;
  waitingProducers: Array<{
    payload: QueuePayload;
    options: { maxAttempts?: number };
    resolve: (job: QueueJob) => void;
    reject: (error: Error) => void;
  }>;
  backpressureStrategy: BackpressureStrategy | "UNKNOWN_STRATEGY";
  add: (payload: QueuePayload, options?: { maxAttempts?: number }) => Promise<QueueJob>;
};

const getQueueInternals = (value: Queue<QueuePayload>): QueueInternals =>
  value as unknown as QueueInternals;

const createJob = (id: string, email: string): QueueJob => ({
  id,
  payload: { email },
  attempts: 0,
  maxAttempts: 3,
  status: "pending",
  nextAttemptAt: null,
  error: null,
});

const createMockStorage = (): jest.Mocked<StorageAdapter<QueuePayload>> => ({
  connect: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
  enqueue: jest.fn().mockResolvedValue(true),
  claim: jest.fn().mockResolvedValue(null),
  renewLease: jest.fn().mockResolvedValue(true),
  complete: jest.fn().mockResolvedValue(true),
  fail: jest.fn().mockResolvedValue(true),
  retry: jest.fn().mockResolvedValue(true),
  peek: jest.fn().mockResolvedValue(null),
  size: jest.fn().mockResolvedValue(0),
  isFull: jest.fn().mockResolvedValue(false),
  waitForCapacity: jest.fn().mockResolvedValue(true),
  isEmpty: jest.fn().mockResolvedValue(true),
  promoteDelayedJobs: jest.fn().mockResolvedValue(0),
  recoverExpiredJobs: jest.fn().mockResolvedValue(0),
  getJob: jest.fn().mockResolvedValue(null),
  getProcessingJobs: jest.fn().mockResolvedValue([]),
});

describe("Queue", () => {
  let queue: Queue<QueuePayload>;

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

    it("should expose provided storage adapter", () => {
      const storage = createMockStorage();
      queue = new Queue("test-queue", { storage });

      expect(queue.getStorage()).toBe(storage);
    });
  });

  describe("connection lifecycle", () => {
    it("should connect and disconnect only once while emitting lifecycle events", async () => {
      const storage = createMockStorage();
      const connectedHandler = jest.fn();
      const disconnectedHandler = jest.fn();

      queue = new Queue("test-queue", { storage });
      queue.on("queue:connected", connectedHandler);
      queue.on("queue:disconnected", disconnectedHandler);

      await queue.connect();
      await queue.connect();
      await queue.disconnect();
      await queue.disconnect();

      expect(storage.connect.mock.calls).toHaveLength(1);
      expect(storage.disconnect.mock.calls).toHaveLength(1);
      expect(connectedHandler).toHaveBeenCalledTimes(1);
      expect(disconnectedHandler).toHaveBeenCalledTimes(1);
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

    it("should auto-connect and default maxAttempts to 3 when options are omitted", async () => {
      queue = new Queue("auto-connect-queue", { capacity: 10 });

      const job = await queue.add({ email: "default@test.com" });

      expect(job.maxAttempts).toBe(3);
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

    it("should emit dropped event with DROP_NEWEST reason when full", async () => {
      const droppedHandler = jest.fn();

      queue = new Queue("test-queue", {
        capacity: 1,
        backpressureStrategy: BackpressureStrategy.DROP_NEWEST,
      });
      queue.on("job:dropped", droppedHandler);
      await queue.connect();

      await queue.add({ email: "user1@test.com" }, { maxAttempts: 3 });
      await expect(queue.add({ email: "user2@test.com" }, { maxAttempts: 3 })).rejects.toThrow(
        "Queue is full. Job dropped (DROP_NEWEST)."
      );

      const [droppedEvent] = droppedHandler.mock.calls[0] as [{ job: QueueJob; reason: string }];

      expect(droppedEvent.job.payload).toEqual({ email: "user2@test.com" });
      expect(droppedEvent.reason).toBe("DROP_NEWEST");
    });

    it("should drop the oldest pending job and add the new job with DROP_OLDEST", async () => {
      const droppedHandler = jest.fn();
      const addedHandler = jest.fn();

      queue = new Queue("test-queue", {
        capacity: 1,
        backpressureStrategy: BackpressureStrategy.DROP_OLDEST,
      });
      queue.on("job:dropped", droppedHandler);
      queue.on("job:added", addedHandler);
      await queue.connect();

      const firstJob = await queue.add({ email: "user1@test.com" }, { maxAttempts: 3 });
      const replacementJob = await queue.add({ email: "user2@test.com" }, { maxAttempts: 4 });

      // The replacement should have succeeded and the first job should be dropped
      expect(replacementJob.payload.email).toBe("user2@test.com");
      expect(replacementJob.maxAttempts).toBe(4);
      expect(await queue.getSize()).toBe(1);
      expect(droppedHandler).toHaveBeenCalledWith(
        expect.objectContaining({ reason: "DROP_OLDEST" })
      );
      expect(addedHandler).toHaveBeenLastCalledWith(replacementJob);
    });

    it("should retry enqueueing immediately when queue is no longer full", async () => {
      const storage = createMockStorage();
      storage.enqueue.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
      storage.isFull.mockResolvedValue(false);

      queue = new Queue("test-queue", {
        storage,
        backpressureStrategy: BackpressureStrategy.DROP_OLDEST,
      });

      await queue.connect();
      const job = await queue.add({ email: "retry@test.com" }, { maxAttempts: 5 });

      expect(job.payload.email).toBe("retry@test.com");
      expect(storage.claim.mock.calls).toHaveLength(0);
      expect(storage.enqueue.mock.calls).toHaveLength(2);
    });

    it("should wait for the enqueue lock before retrying DROP_OLDEST", async () => {
      jest.useFakeTimers();

      const storage = createMockStorage();
      storage.enqueue.mockResolvedValue(true);
      storage.isFull.mockResolvedValue(false);

      queue = new Queue("test-queue", {
        storage,
        backpressureStrategy: BackpressureStrategy.DROP_OLDEST,
      });

      const internals = getQueueInternals(queue);

      internals.enqueueLock = true;
      const dropOldestPromise = internals.dropOldestAndEnqueue({
        id: "locked-job",
        payload: { email: "locked@test.com" },
        attempts: 0,
        maxAttempts: 3,
        status: "pending",
        nextAttemptAt: null,
        error: null,
      });

      setTimeout(() => {
        internals.enqueueLock = false;
      }, 5);

      jest.advanceTimersByTime(10);
      const job = await dropOldestPromise;
      jest.useRealTimers();

      expect(job.payload.email).toBe("locked@test.com");
      expect(storage.enqueue.mock.calls.length).toBeGreaterThan(0);
    });

    it("should fail DROP_OLDEST when no pending job can be claimed", async () => {
      const storage = createMockStorage();
      const droppedHandler = jest.fn();
      storage.enqueue.mockResolvedValue(false);
      storage.isFull.mockResolvedValue(true);
      storage.claim.mockResolvedValue(null);

      queue = new Queue("test-queue", {
        storage,
        backpressureStrategy: BackpressureStrategy.DROP_OLDEST,
      });
      queue.on("job:dropped", droppedHandler);

      await queue.connect();

      await expect(queue.add({ email: "blocked@test.com" }, { maxAttempts: 3 })).rejects.toThrow(
        "No pending jobs to drop"
      );

      const [droppedEvent] = droppedHandler.mock.calls[0] as [{ job: QueueJob; reason: string }];

      expect(droppedEvent.job.payload).toEqual({ email: "blocked@test.com" });
      expect(droppedEvent.reason).toBe("DROP_OLDEST_FAILED");
    });

    it("should throw when replacement enqueue fails after dropping oldest", async () => {
      const storage = createMockStorage();
      const droppedJob = createJob("oldest-job", "oldest@test.com");

      storage.enqueue
        .mockResolvedValueOnce(false) // initial enqueue fails (full)
        .mockResolvedValueOnce(false); // replacement enqueue also fails
      storage.isFull.mockResolvedValue(true);
      storage.claim.mockResolvedValue({
        job: droppedJob,
        claimToken: "drop-token",
      });

      queue = new Queue("test-queue", {
        storage,
        backpressureStrategy: BackpressureStrategy.DROP_OLDEST,
      });

      await queue.connect();

      await expect(
        queue.add({ email: "replacement@test.com" }, { maxAttempts: 3 })
      ).rejects.toThrow("Failed to enqueue new job even after dropping oldest job.");

      // Should have called fail on the dropped job
      expect(storage.fail).toHaveBeenCalledWith(
        "test-queue",
        "oldest-job",
        "drop-token",
        "Dropped: DROP_OLDEST strategy"
      );
    });

    it("should block producers until queued work is drained", async () => {
      queue = new Queue("test-queue", {
        capacity: 1,
        backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
      });
      await queue.connect();

      const storage = queue.getStorage() as MemoryStorageAdapter<QueuePayload>;
      const internals = getQueueInternals(queue);

      await queue.add({ email: "user1@test.com" }, { maxAttempts: 2 });
      const waitingJobPromise = queue.add({ email: "user2@test.com" }, { maxAttempts: 5 });

      await Promise.resolve();

      // Claim and complete the active job to free up space
      const claim = await storage.claim("test-queue", {
        workerId: "test-worker",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });
      expect(claim?.job.payload.email).toBe("user1@test.com");

      await storage.complete("test-queue", claim!.job.id, claim!.claimToken);

      await expect(waitingJobPromise).resolves.toEqual(
        expect.objectContaining({
          payload: { email: "user2@test.com" },
          maxAttempts: 5,
        })
      );
    });

    it("should reject blocked producers when draining cannot re-enqueue", async () => {
      const storage = createMockStorage();
      storage.enqueue
        .mockResolvedValueOnce(false)
        .mockRejectedValueOnce(new Error("enqueue failed while draining"));
      storage.waitForCapacity.mockResolvedValue(true);

      queue = new Queue("test-queue", {
        storage,
        backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
      });

      await queue.connect();
      const waitingJobPromise = queue.add({ email: "waiting@test.com" }, { maxAttempts: 4 });

      await expect(waitingJobPromise).rejects.toThrow("enqueue failed while draining");
    });

    it("should wrap non-Error drain failures in an Error instance", async () => {
      const storage = createMockStorage();
      storage.enqueue.mockResolvedValueOnce(false).mockRejectedValueOnce("string failure");
      storage.waitForCapacity.mockResolvedValue(true);

      queue = new Queue("test-queue", {
        storage,
        backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
      });

      await queue.connect();
      const waitingJobPromise = queue.add({ email: "waiting@test.com" }, { maxAttempts: 4 });

      await expect(waitingJobPromise).rejects.toThrow("string failure");
    });

    it("should leave waiting producers queued when storage is still full", async () => {
      const storage = createMockStorage();
      storage.enqueue.mockResolvedValue(false);
      storage.waitForCapacity.mockResolvedValue(false);

      queue = new Queue("test-queue", {
        storage,
        backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
      });

      const internals = getQueueInternals(queue);

      await queue.connect();
      const blockedAdd = queue.add({ email: "waiting@test.com" }, { maxAttempts: 4 });
      await Promise.resolve();

      expect(internals.waitingProducers).toHaveLength(1);

      await queue.disconnect();
      await expect(blockedAdd).rejects.toThrow("Queue disconnected while producer was blocked.");
      queue = undefined as unknown as Queue<QueuePayload>;
    });

    it("should reject blocked producers when queue disconnects", async () => {
      const storage = createMockStorage();
      storage.enqueue.mockResolvedValue(false);
      storage.waitForCapacity.mockImplementation(() => new Promise<boolean>(() => undefined));

      queue = new Queue("test-queue", {
        storage,
        backpressureStrategy: BackpressureStrategy.BLOCK_PRODUCER,
      });

      await queue.connect();
      const waitingJobPromise = queue.add({ email: "waiting@test.com" }, { maxAttempts: 4 });
      await Promise.resolve();

      await queue.disconnect();

      await expect(waitingJobPromise).rejects.toThrow(
        "Queue disconnected while producer was blocked."
      );
    });

    it("should fall back to the default queue full error for unknown strategies", async () => {
      const storage = createMockStorage();
      storage.enqueue.mockResolvedValue(false);

      queue = new Queue("test-queue", { storage });
      getQueueInternals(queue).backpressureStrategy = "UNKNOWN_STRATEGY";

      await queue.connect();

      await expect(queue.add({ email: "user@test.com" }, { maxAttempts: 3 })).rejects.toThrow(
        "Queue is full."
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
