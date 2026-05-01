import { randomUUID } from "node:crypto";
import type { Job } from "../types/Job";
import type { Claim, ClaimOptions, StorageAdapter } from "./StorageAdapter";

type ProcessingLease = {
  workerId: string;
  claimToken: string;
  leaseUntil: number;
  claimedAt: number;
};

type WaitingClaimer<T> = {
  option: ClaimOptions;
  resolve: (claim: Claim<T> | null) => void;
  timeoutId: NodeJS.Timeout;
};

type CapacityWaiter = {
  resolve: (available: boolean) => void;
  timeoutId: NodeJS.Timeout;
};

export class MemoryStorageAdapter<T> implements StorageAdapter<T> {
  private pendingQueue: string[] = []; // Job IDs in FIFO order
  private jobs: Map<string, Job<T>> = new Map(); // All job data
  private processingJobs: Map<string, ProcessingLease> = new Map(); // jobId -> startedAt timestamp
  private delayedJobs: Map<string, number> = new Map(); // jobId -> runAt timestamp
  private waitingClaimers: WaitingClaimer<T>[] = [];
  private waitingCapacity: CapacityWaiter[] = [];
  private capacity: number;

  // private waitingConsumers: Array<(job: Job<T> | null) => void> = [];

  constructor(capacity: number = 1000) {
    this.capacity = capacity;
  }

  async connect(): Promise<void> {
    // No-op for memory storage
  }

  async disconnect(): Promise<void> {
    // Resolve pending consumers so dequeue promises complete and their timers are cleared.
    for (const waiter of this.waitingClaimers) {
      waiter.resolve(null);
    }

    for (const waiter of this.waitingCapacity) {
      waiter.resolve(false);
    }

    // clear all data
    this.waitingClaimers = [];
    this.waitingCapacity = [];
    this.pendingQueue = [];
    this.jobs.clear();
    this.processingJobs.clear();
    this.delayedJobs.clear();
    // this.waitingConsumers = [];
  }

  async enqueue(_queueName: string, job: Job<T>): Promise<boolean> {
    if (this.pendingQueue.length >= this.capacity) {
      return false;
    }

    const now = Date.now();

    job.status = "pending";
    job.nextAttemptAt = null;
    job.updatedAt = now;
    job.createdAt = job.createdAt || now;
    job.workerId = undefined;
    job.claimedAt = undefined;
    job.leaseUntil = undefined;
    job.claimToken = undefined;
    job.error = job.error || null;

    // If consumers are waiting, deliver the job immediately
    // if (this.waitingConsumers.length > 0) {
    //   this.jobs.set(job.id, job);

    //   const resolver = this.waitingConsumers.shift()!;

    //   job.status = "processing";
    //   job.attempts++;
    //   job.processingStartedAt = now;
    //   job.updatedAt = now;

    //   this.processingJobs.set(job.id, now);

    //   resolver(job);
    //   return true;
    // }

    // Check capacity

    // if (this.queue.length >= this.capacity) {
    //   return false;
    // }

    this.jobs.set(job.id, job);
    this.pendingQueue.push(job.id);

    this.drainWaitingClaimers();

    return true;
  }

  async claim(queueName: string, options: ClaimOptions): Promise<Claim<T> | null> {
    const claimed = this.tryClaim(options);

    //const now = Date.now();
    if (claimed) {
      return claimed;
    }

    const waitingTimeoutMs = options.waitTimeoutMs ?? 5000;
    if (waitingTimeoutMs === 0) {
      return null;
    }

    return new Promise<Claim<T> | null>((resolve) => {
      const timeoutId = setTimeout(() => {
        const index = this.waitingClaimers.findIndex((w) => w.resolve === resolveWrapped);
        if (index !== -1) {
          this.waitingClaimers.splice(index, 1);
        }
        resolve(null);
      }, waitingTimeoutMs);

      const resolveWrapped = (claim: Claim<T> | null) => {
        clearInterval(timeoutId);
        resolve(claim);
      };

      this.waitingClaimers.push({
        option: options,
        resolve: resolveWrapped,
        timeoutId,
      });
    });

    // if (this.queue.length > 0) {
    //   const jobId = this.queue.shift()!;
    //   const job = this.jobs.get(jobId)!;

    //   if (!job) {
    //     return this.dequeue(queueName, timeout);
    //   }

    //   job.status = "processing";
    //   job.attempts++;
    //   job.processingStartedAt = now;
    //   job.updatedAt = now;
    //   this.processingJobs.set(jobId, now);
    //   return job;
    // }

    // if (timeout === 0) {
    //   return null;
    // }

    // return new Promise<Job<T> | null>((resolve) => {
    //   const wrappedResolver = (job: Job<T> | null) => {
    //     clearTimeout(timeoutId);
    //     resolve(job);
    //   };

    //   const timeoutId = setTimeout(() => {
    //     const index = this.waitingConsumers.indexOf(wrappedResolver);

    //     if (index !== -1) {
    //       this.waitingConsumers.splice(index, 1);
    //     }

    //     resolve(null);
    //   }, timeout * 1000);

    //   this.waitingConsumers.push(wrappedResolver);
    // });
  }

  async waitForCapacity(_queueName: string, timeoutMs: number): Promise<boolean> {
    if (this.pendingQueue.length < this.capacity) {
      return true;
    }

    if (timeoutMs <= 0) {
      return false;
    }

    return new Promise<boolean>((resolve) => {
      const resolveWrapped = (available: boolean) => {
        clearTimeout(timeoutId);
        resolve(available);
      };

      const timeoutId = setTimeout(() => {
        const index = this.waitingCapacity.findIndex((waiter) => waiter.resolve === resolveWrapped);
        if (index !== -1) {
          this.waitingCapacity.splice(index, 1);
        }
        resolve(false);
      }, timeoutMs);

      this.waitingCapacity.push({
        resolve: resolveWrapped,
        timeoutId,
      });
    });
  }

  async renewLease(
    queueName: string,
    jobId: string,
    claimToken: string,
    leaseMs: number
  ): Promise<boolean> {
    const lease = this.processingJobs.get(jobId);
    const job = this.jobs.get(jobId);

    if (!lease || !job) {
      return false;
    }

    if (lease.claimToken !== claimToken || job.status !== "processing") {
      return false;
    }

    const now = Date.now();
    const leaseUntil = now + leaseMs;

    lease.leaseUntil = leaseUntil;
    job.leaseUntil = leaseUntil;
    job.updatedAt = now;

    return true;
  }

  async complete(queueName: string, jobId: string, claimToken: string): Promise<boolean> {
    const job = this.jobs.get(jobId);
    if (!job || !this.hasActiveClaim(jobId, claimToken)) {
      return false;
    }

    const now = Date.now();

    job.status = "completed";
    job.updatedAt = now;
    // job.processingStartedAt = undefined;
    job.nextAttemptAt = null;
    this.clearClaimMetadata(job);

    this.processingJobs.delete(jobId);

    return true;
  }

  async fail(
    queueName: string,
    jobId: string,
    claimToken: string,
    error?: string
  ): Promise<boolean> {
    const job = this.jobs.get(jobId);

    if (!job || !this.hasActiveClaim(jobId, claimToken)) {
      return false;
    }

    const now = Date.now();

    job.status = "failed";
    job.error = error ?? null;
    job.updatedAt = now;
    job.nextAttemptAt = null;

    this.clearClaimMetadata(job);

    this.processingJobs.delete(jobId);

    return true;
  }

  async retry(
    queueName: string,
    jobId: string,
    claimToken: string,
    executeAt: number,
    error?: string
  ): Promise<boolean> {
    const job = this.jobs.get(jobId);

    if (!job || !this.hasActiveClaim(jobId, claimToken)) {
      return false;
    }

    const now = Date.now();

    job.status = "delayed";
    job.error = error ?? null;
    job.nextAttemptAt = new Date(executeAt);
    job.updatedAt = now;
    this.clearClaimMetadata(job);

    this.processingJobs.delete(jobId);
    this.delayedJobs.set(jobId, executeAt);

    return true;
  }

  async promoteDelayedJobs(_queueName: string, now: number = Date.now()): Promise<number> {
    // const now = Date.now();
    let promoted = 0;

    for (const [jobId, executeAt] of this.delayedJobs) {
      if (executeAt > now) {
        continue;
      }

      const job = this.jobs.get(jobId);
      this.delayedJobs.delete(jobId);

      if (!job) continue;

      job.status = "pending";
      job.nextAttemptAt = null;
      job.updatedAt = now;

      // if (this.waitingConsumers.length > 0) {
      //   const resolver = this.waitingConsumers.shift()!;

      //   job.status = "processing";
      //   job.attempts++;
      //   job.processingStartedAt = now;
      //   job.updatedAt = now;
      //   this.processingJobs.set(job.id, now);

      //   resolver(job);
      // } else {
      //   this.queue.push(jobId);
      // }
      // promoted++;

      this.pendingQueue.push(jobId);
      promoted++;
    }
    if (promoted > 0) {
      this.drainWaitingClaimers();
    }
    return promoted;
  }

  async recoverExpiredJobs(queueName: string, now: number): Promise<number> {
    let recovered = 0;

    for (const [jobId, lease] of this.processingJobs) {
      if (lease.leaseUntil > now) {
        continue;
      }
      const job = this.jobs.get(jobId);
      this.processingJobs.delete(jobId);

      if (!job) continue;

      if (job.status !== "processing") {
        continue;
      }

      job.status = "pending";
      job.updatedAt = now;
      this.clearClaimMetadata(job);

      this.pendingQueue.push(jobId);
      recovered++;
    }
    if (recovered > 0) {
      this.drainWaitingClaimers();
    }
    return recovered;
  }

  async getJob(_queueName: string, jobId: string): Promise<Job<T> | null> {
    return this.jobs.get(jobId) ?? null;
  }

  async peek(_queueName: string): Promise<Job<T> | null> {
    if (this.pendingQueue.length === 0) {
      return null;
    }
    const jobId = this.pendingQueue[0];
    return this.jobs.get(jobId) || null;
  }

  async size(_queueName: string): Promise<number> {
    return this.pendingQueue.length;
  }

  async isFull(_queueName: string): Promise<boolean> {
    return this.pendingQueue.length >= this.capacity;
  }

  async isEmpty(_queueName: string): Promise<boolean> {
    return this.pendingQueue.length === 0;
  }
  async getProcessingJobs(_queueName: string): Promise<string[]> {
    return Array.from(this.processingJobs.keys());
  }

  private tryClaim(options: ClaimOptions): Claim<T> | null {
    while (this.pendingQueue.length > 0) {
      const jobId = this.pendingQueue.shift()!;
      const job = this.jobs.get(jobId);

      if (!job) {
        continue;
      }

      if (job.status !== "pending") {
        continue;
      }

      const now = Date.now();
      const claimToken = randomUUID();
      const leaseUntil = now + options.leaseMs;

      job.status = "processing";
      job.attempts = 1;
      job.workerId = options.workerId;
      job.claimedAt = now;
      job.leaseUntil = leaseUntil;
      job.claimToken = claimToken;
      job.updatedAt = now;
      job.nextAttemptAt = null;

      this.processingJobs.set(jobId, {
        workerId: options.workerId,
        claimToken,
        claimedAt: now,
        leaseUntil,
      });

      this.notifyCapacityAvailable();

      return {
        job,
        claimToken,
      };
    }
    return null;
  }

  private hasActiveClaim(jobId: string, claimToken: string): boolean {
    const lease = this.processingJobs.get(jobId);
    if (!lease) {
      return false;
    }
    return lease.claimToken === claimToken;
  }

  private clearClaimMetadata(job: Job<T>): void {
    job.workerId = undefined;
    job.claimedAt = undefined;
    job.leaseUntil = undefined;
    job.claimToken = undefined;
  }

  private drainWaitingClaimers(): void {
    while (this.waitingClaimers.length > 0) {
      const worker = this.waitingClaimers[0];
      const claimed = this.tryClaim(worker.option);

      if (!claimed) {
        break;
      }

      this.waitingClaimers.shift();
      clearTimeout(worker.timeoutId);
      worker.resolve(claimed);
    }
  }

  private notifyCapacityAvailable(): void {
    if (this.pendingQueue.length >= this.capacity) {
      return;
    }

    const waiters = this.waitingCapacity.splice(0);
    for (const waiter of waiters) {
      clearTimeout(waiter.timeoutId);
      waiter.resolve(true);
    }
  }

  // async scheduleDelayed(_queueName: string, job: Job<T>, executeAt: number): Promise<void> {
  //   const now = Date.now();

  //   job.status = "pending";
  //   job.nextAttemptAt = new Date(executeAt);
  //   job.createdAt = now;
  //   job.processingStartedAt = undefined;
  //   job.workerId = undefined;

  //   this.processingJobs.delete(job.id);
  //   this.delayedJobs.set(job.id, executeAt);
  //   this.jobs.set(job.id, job);
  // }

  // async markProcessing(_queueName: string, jobId: string, workerId: string): Promise<void> {
  //   const now = Date.now();
  //   const job = this.jobs.get(jobId);

  //   if (!job) return;

  //   job.status = "processing";
  //   job.processingStartedAt = now;
  //   job.workerId = workerId;
  //   job.updatedAt = now;

  //   this.processingJobs.set(jobId, now);
  // }

  // async markCompleted(_queueName: string, jobId: string): Promise<void> {
  //   const now = Date.now();
  //   const job = this.jobs.get(jobId);

  //   if (!job) return;

  //   job.status = "completed";
  //   job.processingStartedAt = undefined;
  //   job.updatedAt = now;

  //   this.processingJobs.delete(jobId);
  // }

  // async markFailed(_queueName: string, jobId: string, error?: string): Promise<void> {
  //   const now = Date.now();
  //   const job = this.jobs.get(jobId);

  //   if (!job) return;

  //   job.status = "failed";
  //   job.processingStartedAt = undefined;
  //   job.error = error || null;
  //   job.updatedAt = now;

  //   this.processingJobs.delete(jobId);
  // }

  // async updateJob(_queueName: string, job: Job<T>): Promise<void> {
  //   job.updatedAt = Date.now();
  //   this.jobs.set(job.id, job);

  //   if (job.status === "processing" && job.processingStartedAt !== undefined) {
  //     this.processingJobs.set(job.id, job.processingStartedAt);
  //   }
  // }

  // async recoverStuckJobs(_queueName: string, timeoutMs: number): Promise<number> {
  //   const now = Date.now();
  //   let recovered = 0;

  //   for (const [jobId, startedAt] of this.processingJobs) {
  //     if (now - startedAt >= timeoutMs) {
  //       const job = this.jobs.get(jobId);

  //       if (!job) {
  //         this.processingJobs.delete(jobId);
  //         continue;
  //       }

  //       job.status = "pending";
  //       job.processingStartedAt = undefined;
  //       job.workerId = undefined;
  //       job.updatedAt = now;

  //       this.processingJobs.delete(jobId);
  //       this.queue.unshift(jobId);
  //       recovered++;
  //     }
  //   }
  //   return recovered;
  // }

  getStats(): { pending: number; processing: number; delayed: number; total: number } {
    return {
      pending: this.pendingQueue.length,
      processing: this.processingJobs.size,
      delayed: this.delayedJobs.size,
      total: this.jobs.size,
    };
  }
}
