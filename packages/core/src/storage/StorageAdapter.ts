import { Job } from "../types/Job";

export interface StorageAdapter<T> {
    // Connection lifecycle
    connect(): Promise<void>;
    disconnect(): Promise<void>;

    // Queue operations
    enqueue(queueName: string, job: Job<T>): Promise<boolean>;
    dequeue(queueName: string, timeout?: number): Promise<Job<T> | null>;
    peek(queueName: string): Promise<Job<T> | null>;
    size(queueName: string): Promise<number>;
    isFull(queueName: string): Promise<boolean>;
    isEmpty(queueName: string): Promise<boolean>;

    // Delayed job operations
    scheduleDelayed(queueName: string, job: Job<T>, executeAt: number): Promise<void>;
    promoteDelayedJobs(queueName: string): Promise<number>;

    // Job lifecycle
    markProcessing(queueName: string, jobId: string, workerId: string): Promise<void>;
    markCompleted(queueName: string, jobId: string): Promise<void>;
    markFailed(queueName: string, jobId: string, error?: string): Promise<void>;
    // Job data access
    getJob(queueName: string, jobId: string): Promise<Job<T> | null>;
    updateJob(queueName: string, job: Job<T>): Promise<void>;

    // Recovery
    recoverStuckJobs(queueName: string, timeoutMs: number): Promise<number>;
    getProcessingJobs(queueName: string): Promise<string[]>;
}