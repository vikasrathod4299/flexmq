import type { Job } from "./Job";

interface JobLifeCycleEvents<T> {
  "job:added": { job: Job<T> };
  "job:completed": { job: Job<T>; result: unknown };
  "job:failed": { job: Job<T>; error: Error };
  "job:dropped": { job: Job<T>; reason: string };
}

interface QueueStateEvents {
  "queue:full": void;
  "queue:drained": void;
  "queue:dropped": { reason: string };
}

interface WorkerEvents {
  "worker:idle": void;
  "worker:busy": void;
}

export type QueueEventMap<T> = JobLifeCycleEvents<T> & QueueStateEvents & WorkerEvents;
