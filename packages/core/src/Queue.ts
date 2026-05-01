import { EventEmitter } from "node:events";
import type { Job } from "./types/Job";
import { randomUUID } from "node:crypto";
import type { StorageAdapter } from "./storage/StorageAdapter";
import BackpressureStrategy from "./queue/BackpressureStrategy";
import { getMemoryStorage } from "./storage/StorageRegistry";

export interface QueueOptions<T> {
  storage?: StorageAdapter<T>;
  capacity?: number;
  backpressureStrategy?: BackpressureStrategy;
}

export class Queue<T> extends EventEmitter {
  private storage: StorageAdapter<T>;
  private queueName: string;
  private capacity: number;
  private backpressureStrategy: BackpressureStrategy;
  private isConnected: boolean = false;

  // Locks and flags for backpressure handling
  private enqueueLock: boolean = false;
  private drainingProducers: boolean = false;
  private producerWaitTimeoutMs: number = 5000;

  private waitingProducers: Array<{
    payload: T;
    options: { maxAttempts?: number };
    resolve: (job: Job<T>) => void;
    reject: (error: Error) => void;
  }> = [];

  constructor(queueName: string, options: QueueOptions<T> = {}) {
    super();
    this.queueName = queueName;
    this.capacity = options.capacity ?? 1000;
    this.backpressureStrategy = options.backpressureStrategy ?? BackpressureStrategy.BLOCK_PRODUCER;

    // Use provided storage or fall back to in-memory
    this.storage = options.storage ?? getMemoryStorage<T>(queueName, this.capacity);
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;
    await this.storage.connect();
    this.isConnected = true;
    this.emit("queue:connected");
  }

  async disconnect(): Promise<void> {
    if (!this.isConnected) return;

    const waitingProducers = this.waitingProducers.splice(0);
    for (const waiter of waitingProducers) {
      waiter.reject(new Error("Queue disconnected while producer was blocked."));
    }

    await this.storage.disconnect();
    this.isConnected = false;
    this.emit("queue:disconnected");
  }

  async add(payload: T, options: { maxAttempts?: number } = {}): Promise<Job<T>> {
    if (!this.isConnected) await this.connect();

    const job: Job<T> = {
      id: randomUUID(),
      payload,
      attempts: 0,
      maxAttempts: options?.maxAttempts ?? 3,
      status: "pending",
      nextAttemptAt: null,
      error: null,
    };

    const added = await this.storage.enqueue(this.queueName, job);

    if (added) {
      this.emit("job:added", job);
      await this.drainWaitingProducers();
      return job;
    }

    return this.handleBackpressure(job);
  }

  private async handleBackpressure(job: Job<T>): Promise<Job<T>> {
    switch (this.backpressureStrategy) {
      case BackpressureStrategy.DROP_NEWEST: {
        this.emit("job:dropped", { job, reason: "DROP_NEWEST" });
        throw new Error("Queue is full. Job dropped (DROP_NEWEST).");
      }

      case BackpressureStrategy.DROP_OLDEST: {
        return this.dropOldestAndEnqueue(job);
      }

      case BackpressureStrategy.BLOCK_PRODUCER: {
        const promise = new Promise<Job<T>>((resolve, reject) => {
          this.waitingProducers.push({
            payload: job.payload,
            options: { maxAttempts: job.maxAttempts },
            resolve,
            reject,
          });
        });

        void this.drainWaitingProducers();

        return promise;
      }

      case BackpressureStrategy.ERROR: {
        throw new Error("Queue is full. Job cannot be added.");
      }

      default:
        throw new Error("Queue is full.");
    }
  }

  private async dropOldestAndEnqueue(job: Job<T>): Promise<Job<T>> {
    while (this.enqueueLock) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    this.enqueueLock = true;

    try {
      const still_full = await this.storage.isFull(this.queueName);
      if (!still_full) {
        const added = await this.storage.enqueue(this.queueName, job);
        if (added) {
          this.emit("job:added", job);
          return job;
        }
      }
      const droppedClaim = await this.storage.claim(this.queueName, {
        workerId: "queue:drop-oldest",
        leaseMs: 60000, // long lease to ensure it doesn't get processed by a real worker
        waitTimeoutMs: 1000, // short wait to avoid blocking too long if claim fails
      });

      if (!droppedClaim) {
        // No pending jobs to drop (all are processing), can't apply DROP_OLDEST
        this.emit("job:dropped", { job, reason: "DROP_OLDEST_FAILED" });
        throw new Error("Queue is full. No pending jobs to drop (all jobs are processing).");
      }

      this.storage.fail(
        this.queueName,
        droppedClaim.job.id,
        droppedClaim.claimToken,
        "Dropped: DROP_OLDEST strategy"
      );
      this.emit("job:dropped", { job: droppedClaim.job, reason: "DROP_OLDEST" });

      const added = await this.storage.enqueue(this.queueName, job);

      if (added) {
        this.emit("job:added", job);
        return job;
      }

      // Re-enqueue the dropped job so we don't lose it silently.
      // await this.storage.enqueue(this.queueName, droppedClaim.job);
      throw new Error("Queue is full. Failed to enqueue new job even after dropping oldest job.");
    } finally {
      this.enqueueLock = false;
    }
  }

  /**
   * Called after a job is dequeued and processed/completed (space freed).
   * Tries to enqueue the next waiting producer's job.
   */
  private async drainWaitingProducers(): Promise<void> {
    if (this.drainingProducers) return;
    if (this.waitingProducers.length === 0) return;

    this.drainingProducers = true;

    try {
      while (this.waitingProducers.length > 0) {
        if (!this.isConnected) {
          return;
        }

        const waiter = this.waitingProducers[0];
        const job: Job<T> = {
          id: randomUUID(),
          payload: waiter.payload,
          attempts: 0,
          maxAttempts: waiter.options?.maxAttempts ?? 3,
          status: "pending",
          nextAttemptAt: null,
          error: null,
        };

        try {
          const added = await this.storage.enqueue(this.queueName, job);
          if (added) {
            this.waitingProducers.shift();
            this.emit("job:added", job);
            waiter.resolve(job);
            continue;
          }

          const capacityAvailable = await this.storage.waitForCapacity(
            this.queueName,
            this.producerWaitTimeoutMs
          );

          if (!capacityAvailable) {
            continue;
          }
        } catch (error) {
          this.waitingProducers.shift();
          waiter.reject(error instanceof Error ? error : new Error(String(error)));
        }
      }
    } finally {
      this.drainingProducers = false;

      if (this.waitingProducers.length > 0) {
        void this.drainWaitingProducers();
      }
    }
  }

  /** Get the underlying storage adapter (useful for advanced usage) */
  getStorage(): StorageAdapter<T> {
    return this.storage;
  }

  async getJob(jobId: string): Promise<Job<T> | null> {
    return this.storage.getJob(this.queueName, jobId);
  }

  async getSize(): Promise<number> {
    return this.storage.size(this.queueName);
  }

  getName(): string {
    return this.queueName;
  }
}
