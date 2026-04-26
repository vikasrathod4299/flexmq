export type JobStatus = "pending" | "processing" | "delayed" | "completed" | "failed";

export interface Job<T> {
  id: string;
  payload: T;

  status: JobStatus;
  attempts: number;
  maxAttempts: number;

  error: string | null;
  nextAttemptAt: Date | null;

  createdAt?: number;
  updatedAt?: number;


  workerId?: string;
  claimedAt?: number;
  leaseUntil?: number;

  claimToken?: string;
  processingStartedAt?: number;
}