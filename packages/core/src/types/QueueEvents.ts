import type { Job } from "./Job";

export interface QueueEventMap<T> {
  "queue:connected": void;
  "queue:disconnected": void;
  "job:added": Job<T>;
  "job:dropped": { job: Job<T>; reason: string };
}

export interface WorkerEventMap<T> {
  "worker:started": { queueName: string; concurrency: number };
  "worker:stopped": { queueName: string };
  "worker:error": { worker?: string; workerId?: string; error: unknown };
  "job:processing": { job: Job<T>; worker: string };
  "job:completed": { job: Job<T>; duration: number };
  "job:retry": { job: Job<T>; error: string; nextAttemptAt: Date };
  "job:failed": { job: Job<T>; error: string };
  "job:lost-claim": {
    job?: Job<T>;
    jobId?: string;
    worker?: string;
    workerId?: string;
    phase: "complete" | "retry" | "fail" | "heartbeat";
    error?: string;
  };
  "jobs:promoted": { count: number };
  "jobs:recovered": { count: number };
}
