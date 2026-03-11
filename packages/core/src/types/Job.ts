export type JobStatus = "pending" | "processing" | "failed" | "completed";

export interface Job<T> {
  id: string;
  payload: T;
  attempts: number;
  maxAttempts: number;
  status: JobStatus;
  nextAttemptAt: Date | null;
  createdAt?: number;
  updatedAt?: number;
  processingStartedAt?: number;
  workerId?: string;
  error: string | null;
}
