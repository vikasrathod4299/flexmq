import { EventEmitter } from "node:events";
import type { Job } from "./types/Job";
import type { StorageAdapter } from "./storage/StorageAdapter";
import Metrics from "./metrics/metrics";
import { getMemoryStorage } from "./storage/StorageRegistry";

export interface WorkerOptions<T> {
  storage?: StorageAdapter<T>;
  concurrency?: number;
  processor: (job: Job<T>) => Promise<void>;
  stuckJobTimeout?: number;
  capacity?: number;
  timeoutMs?: number;
}

export class Worker<T> extends EventEmitter {
  private storage: StorageAdapter<T>;
  private queueName: string;
  private concurrency: number;
  private processor: (job: Job<T>) => Promise<void>;
  private stuckJobTimeout: number;
  private isRunning: boolean = false;
  private activeWorkers: number = 0;
  private metrics: Metrics;
  private timeoutMs: number;

  constructor(queueName: string, options: WorkerOptions<T>) {
    super();
    this.queueName = queueName;
    this.concurrency = options.concurrency ?? 1;
    this.processor = options.processor;
    this.stuckJobTimeout = options.stuckJobTimeout ?? 30000;
    this.metrics = new Metrics();
    this.timeoutMs = options.timeoutMs || 5000;

    // Use provided storage or fall back to in-memory
    this.storage = options.storage ?? getMemoryStorage<T>(queueName, options.capacity);
  }

  async start(): Promise<void> {
    if (this.isRunning) return;

    await this.storage.connect();

    this.isRunning = true;
    this.emit("worker:started", { queueName: this.queueName, concurrency: this.concurrency });

    const recovered = await this.storage.recoverStuckJobs(this.queueName, this.stuckJobTimeout);
    if (recovered > 0) {
      this.emit("worker:recovered", { count: recovered });
    }

    for (let i = 0; i < this.concurrency; i++) {
      void this.workerLoop(i);
    }

    void this.delayedJobLoop();
  }

  async stop(gracefulTimeoutMs: number = 5000): Promise<void> {
    this.isRunning = false;

    const start = Date.now();
    while (this.activeWorkers > 0 && Date.now() - start < gracefulTimeoutMs) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    await this.storage.disconnect();
    this.emit("worker:stopped", { queueName: this.queueName });
  }

  private async workerLoop(workerId: number): Promise<void> {
    const workerName = `${this.queueName}-worker-${workerId}`;

    while (this.isRunning) {
      try {
        const job = await this.storage.dequeue(this.queueName, this.timeoutMs);

        if (job) {
          const size = await this.storage.size(this.queueName);
          this.metrics.updateQueueSize(size);

          this.activeWorkers++;
          this.updateMetrics();
          try {
            await this.processJob(job, workerName);
          } finally {
            this.activeWorkers--;
            this.updateMetrics();
          }
        }
      } catch (error) {
        this.emit("worker:error", { worker: workerName, error });
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  private async processJob(job: Job<T>, workerName: string): Promise<void> {
    const startTime = Date.now();
    this.emit("job:processing", { job, worker: workerName });
    job.status = "processing";

    try {
      await this.processor(job);
      await this.storage.markCompleted(this.queueName, job.id);

      job.status = "completed";
      this.metrics.incrementJobsCompleted();
      this.metrics.recordProcessingTime(Date.now() - startTime);
      this.emit("job:completed", { job, duration: Date.now() - startTime });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);

      if (job.attempts < job.maxAttempts) {
        job.status = "pending";

        const delayMs = Math.pow(2, job.attempts) * 1000;
        const executeAt = Date.now() + delayMs;

        await this.storage.scheduleDelayed(this.queueName, job, executeAt);

        this.metrics.incrementRetries();
        this.emit("job:retry", { job, error: errorMessage, nextAttemptAt: new Date(executeAt) });
      } else {
        job.status = "failed";
        await this.storage.markFailed(this.queueName, job.id, errorMessage);
        this.metrics.incrementJobsFailed();
        this.emit("job:failed", { job, error: errorMessage });
      }
    }
  }

  private async delayedJobLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        const promoted = await this.storage.promoteDelayedJobs(this.queueName);

        if (promoted > 0) {
          this.emit("jobs:promoted", { count: promoted });
          const size = await this.storage.size(this.queueName);
          this.metrics.updateQueueSize(size);
        }
      } catch (error) {
        this.emit("error", { error, context: "delayedJobLoop" });
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  private updateMetrics(): void {
    this.metrics.updateWorkerStats(this.activeWorkers, this.concurrency - this.activeWorkers);
  }

  getMetrics() {
    return this.metrics.getSnapshot();
  }

  isActive(): boolean {
    return this.isRunning;
  }
}
