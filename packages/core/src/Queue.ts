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
        return new Promise((resolve, reject) => {
          this.waitingProducers.push({
            payload: job.payload,
            options: { maxAttempts: job.maxAttempts },
            resolve,
            reject,
          });
        });
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
      const oldestJob = await this.storage.dequeue(this.queueName, 0);
      if (!oldestJob) {
        // No pending jobs to drop (all are processing), can't apply DROP_OLDEST
        this.emit("job:dropped", { job, reason: "DROP_OLDEST_FAILED" });
        throw new Error("Queue is full. No pending jobs to drop (all jobs are processing).");
      }
      const added = await this.storage.enqueue(this.queueName, job);

      if (added) {
        this.emit("job:dropped", { job: oldestJob, reason: "DROP_OLDEST" });
        this.emit("job:added", job);
        return job;
      }

      // Re-enqueue the dropped job so we don't lose it silently.
      await this.storage.enqueue(this.queueName, oldestJob);
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
    this.drainingProducers = true;

    if (this.waitingProducers.length === 0) return;
    if (await this.storage.isFull(this.queueName)) return;

    const waiter = this.waitingProducers.shift()!;

    try {
      const job = await this.add(waiter.payload, waiter.options);
      waiter.resolve(job);
    } catch (error) {
      waiter.reject(error instanceof Error ? error : new Error(String(error)));
    } finally {
      this.drainingProducers = false;
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
