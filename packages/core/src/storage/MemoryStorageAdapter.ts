import type { Job } from "../types/Job";
import type { StorageAdapter } from "./StorageAdapter";

export class MemoryStorageAdapter<T> implements StorageAdapter<T> {
  private queue: string[] = []; // Job IDs in FIFO order
  private jobs: Map<string, Job<T>> = new Map(); // All job data
  private processingJobs: Map<string, number> = new Map(); // jobId -> startedAt timestamp
  private delayedJobs: Map<string, number> = new Map(); // jobId -> runAt timestamp
  private capacity: number;

  private waitingConsumers: Array<(job: Job<T> | null) => void> = [];

  constructor(capacity: number = 1000) {
    this.capacity = capacity;
  }

  async connect(): Promise<void> {
    // No-op for memory storage
  }

  async disconnect(): Promise<void> {
    // Resolve pending consumers so dequeue promises complete and their timers are cleared.
    for (const resolveWaitingConsumer of this.waitingConsumers) {
      resolveWaitingConsumer(null);
    }

    // clear all data
    this.queue = [];
    this.jobs.clear();
    this.processingJobs.clear();
    this.delayedJobs.clear();
    this.waitingConsumers = [];
  }

  async enqueue(_queueName: string, job: Job<T>): Promise<boolean> {
    const now = Date.now();
    job.createdAt = now;
    job.updatedAt = now;
    job.status = "pending";

    // If consumers are waiting, deliver the job immediately
    if (this.waitingConsumers.length > 0) {
      this.jobs.set(job.id, job);

      const resolver = this.waitingConsumers.shift()!;

      job.status = "processing";
      job.attempts++;
      job.processingStartedAt = now;
      job.updatedAt = now;

      this.processingJobs.set(job.id, now);

      resolver(job);
      return true;
    }

    // Check capacity
    if (this.queue.length >= this.capacity) {
      return false;
    }

    this.jobs.set(job.id, job);
    this.queue.push(job.id);
    return true;
  }

  async dequeue(queueName: string, timeout: number = 5): Promise<Job<T> | null> {
    const now = Date.now();

    if (this.queue.length > 0) {
      const jobId = this.queue.shift()!;
      const job = this.jobs.get(jobId)!;

      if (!job) {
        return this.dequeue(queueName, timeout);
      }

      job.status = "processing";
      job.attempts++;
      job.processingStartedAt = now;
      job.updatedAt = now;
      this.processingJobs.set(jobId, now);
      return job;
    }

    if (timeout === 0) {
      return null;
    }

    return new Promise<Job<T> | null>((resolve) => {
      const wrappedResolver = (job: Job<T> | null) => {
        clearTimeout(timeoutId);
        resolve(job);
      };

      const timeoutId = setTimeout(() => {
        const index = this.waitingConsumers.indexOf(wrappedResolver);

        if (index !== -1) {
          this.waitingConsumers.splice(index, 1);
        }

        resolve(null);
      }, timeout * 1000);

      this.waitingConsumers.push(wrappedResolver);
    });
  }

  async peek(_queueName: string): Promise<Job<T> | null> {
    if (this.queue.length === 0) {
      return null;
    }
    const jobId = this.queue[0];
    return this.jobs.get(jobId) || null;
  }

  async size(_queueName: string): Promise<number> {
    return this.queue.length;
  }

  async isFull(_queueName: string): Promise<boolean> {
    return this.queue.length >= this.capacity;
  }

  async isEmpty(_queueName: string): Promise<boolean> {
    return this.queue.length === 0;
  }

  async scheduleDelayed(_queueName: string, job: Job<T>, executeAt: number): Promise<void> {
    const now = Date.now();

    job.status = "pending";
    job.nextAttemptAt = new Date(executeAt);
    job.createdAt = now;
    job.processingStartedAt = undefined;
    job.workerId = undefined;

    this.processingJobs.delete(job.id);
    this.delayedJobs.set(job.id, executeAt);
    this.jobs.set(job.id, job);
  }

  async promoteDelayedJobs(_queueName: string): Promise<number> {
    const now = Date.now();
    let promoted = 0;

    for (const [jobId, executeAt] of this.delayedJobs) {
      if (executeAt <= now) {
        this.delayedJobs.delete(jobId);

        const job = this.jobs.get(jobId);
        if (!job) continue;

        job.nextAttemptAt = null;
        job.updatedAt = now;

        if (this.waitingConsumers.length > 0) {
          const resolver = this.waitingConsumers.shift()!;

          job.status = "processing";
          job.attempts++;
          job.processingStartedAt = now;
          job.updatedAt = now;
          this.processingJobs.set(job.id, now);

          resolver(job);
        } else {
          this.queue.push(jobId);
        }
        promoted++;
      }
    }
    return promoted;
  }

  async markProcessing(_queueName: string, jobId: string, workerId: string): Promise<void> {
    const now = Date.now();
    const job = this.jobs.get(jobId);

    if (!job) return;

    job.status = "processing";
    job.processingStartedAt = now;
    job.workerId = workerId;
    job.updatedAt = now;

    this.processingJobs.set(jobId, now);
  }

  async markCompleted(_queueName: string, jobId: string): Promise<void> {
    const now = Date.now();
    const job = this.jobs.get(jobId);

    if (!job) return;

    job.status = "completed";
    job.processingStartedAt = undefined;
    job.updatedAt = now;

    this.processingJobs.delete(jobId);
  }

  async markFailed(_queueName: string, jobId: string, error?: string): Promise<void> {
    const now = Date.now();
    const job = this.jobs.get(jobId);

    if (!job) return;

    job.status = "failed";
    job.processingStartedAt = undefined;
    job.error = error || null;
    job.updatedAt = now;

    this.processingJobs.delete(jobId);
  }

  async getJob(_queueName: string, jobId: string): Promise<Job<T> | null> {
    return this.jobs.get(jobId) || null;
  }

  async updateJob(_queueName: string, job: Job<T>): Promise<void> {
    job.updatedAt = Date.now();
    this.jobs.set(job.id, job);

    if (job.status === "processing" && job.processingStartedAt !== undefined) {
      this.processingJobs.set(job.id, job.processingStartedAt);
    }
  }

  async recoverStuckJobs(_queueName: string, timeoutMs: number): Promise<number> {
    const now = Date.now();
    let recovered = 0;

    for (const [jobId, startedAt] of this.processingJobs) {
      if (now - startedAt >= timeoutMs) {
        const job = this.jobs.get(jobId);

        if (!job) {
          this.processingJobs.delete(jobId);
          continue;
        }

        job.status = "pending";
        job.processingStartedAt = undefined;
        job.workerId = undefined;
        job.updatedAt = now;

        this.processingJobs.delete(jobId);
        this.queue.unshift(jobId);
        recovered++;
      }
    }
    return recovered;
  }

  async getProcessingJobs(_queueName: string): Promise<string[]> {
    return Array.from(this.processingJobs.keys());
  }

  getStats(): { pending: number; processing: number; delayed: number; total: number } {
    return {
      pending: this.queue.length,
      processing: this.processingJobs.size,
      delayed: this.delayedJobs.size,
      total: this.jobs.size,
    };
  }
}
