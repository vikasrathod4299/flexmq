import type { Job } from "../types/Job";

export interface ClaimOptions {
  workerId: string;
  leaseMs: number;
  waitTimeoutMs?: number;
}

export interface Claim<T> {
  job: Job<T>;
  claimToken: string;
}

export interface StorageAdapter<T> {
  // Connection lifecycle
  connect(): Promise<void>;
  disconnect(): Promise<void>;

  // Queue operations
  enqueue(queueName: string, job: Job<T>): Promise<boolean>;
  getJob(queueName: string, jobId: string): Promise<Job<T> | null>;
  // updateJob(queueName: string, job: Job<T>): Promise<void>;

  claim(queueName: string, options: ClaimOptions): Promise<Claim<T> | null>;
  renewLease(
    queueName: string,
    jobId: string,
    claimToken: string,
    leaseMs: number
  ): Promise<boolean>;

  complete(queueName: string, jobId: string, claimToken: string): Promise<boolean>;
  fail(queueName: string, jobId: string, claimToken: string, error?: string): Promise<boolean>;

  retry(
    queueName: string,
    jobId: string,
    claimToken: string,
    executeAt: number,
    error?: string
  ): Promise<boolean>;

  // Delayed job operations
  promoteDelayedJobs(queueName: string, now?: number): Promise<number>;
  recoverExpiredJobs(queueName: string, now: number): Promise<number>;

  size(queueName: string): Promise<number>;
  isFull(queueName: string): Promise<boolean>;
  waitForCapacity(queueName: string, timeoutMs: number): Promise<boolean>;
  isEmpty(queueName: string): Promise<boolean>;
  peek(queueName: string): Promise<Job<T> | null>;

  // // Delayed job operations
  // scheduleDelayed(queueName: string, job: Job<T>, executeAt: number): Promise<void>; // retry() is new

  // // Job lifecycle
  // markProcessing(queueName: string, jobId: string, workerId: string): Promise<void>; // claim() is new
  // markCompleted(queueName: string, jobId: string): Promise<void>;               // complete() is new
  // markFailed(queueName: string, jobId: string, error?: string): Promise<void>; // fail() is new

  // // Recovery
  // recoverStuckJobs(queueName: string, timeoutMs: number): Promise<number>; // recoverExpiredJobs() is new

  getProcessingJobs(queueName: string): Promise<string[]>;
}
