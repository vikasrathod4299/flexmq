import { EventEmitter } from "node:events";
import type { Job } from "./types/Job";
import type { Claim, StorageAdapter } from "./storage/StorageAdapter";
import Metrics from "./metrics/metrics";
import { getMemoryStorage } from "./storage/StorageRegistry";

export interface WorkerOptions<T> {
  storage?: StorageAdapter<T>;
  concurrency?: number;
  processor: (job: Job<T>) => Promise<void>;
  capacity?: number;

  // stuckJobTimeout?: number;
  // timeoutMs?: number;
  dequeueTimeoutMs?: number;
  leaseMs?: number;
  heartbeatIntervalMs?: number;
  recoveryIntervalMs?: number;
  delayedJobCheckIntervalMs?: number;
}

export class Worker<T> extends EventEmitter {
  private storage: StorageAdapter<T>;
  private queueName: string;
  private concurrency: number;
  private processor: (job: Job<T>) => Promise<void>;

  // private stuckJobTimeout: number;
  private dequeueTimeoutMs: number;
  private leaseMs: number;
  private heartbeatIntervalMs: number;
  private recoveryIntervalMs: number;
  private delayedJobCheckIntervalMs: number;

  private isRunning: boolean = false;
  private activeWorkers: number = 0;
  private metrics: Metrics = new Metrics();

  constructor(queueName: string, options: WorkerOptions<T>) {
    super();
    this.queueName = queueName;
    this.concurrency = options.concurrency ?? 1;
    this.processor = options.processor;

    this.dequeueTimeoutMs = options.dequeueTimeoutMs ?? 5000;
    this.leaseMs = options.leaseMs ?? 30000;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 10000;
    this.recoveryIntervalMs = options.recoveryIntervalMs ?? 5000;
    this.delayedJobCheckIntervalMs = options.delayedJobCheckIntervalMs ?? 250;

    // Use provided storage or fall back to in-memory
    this.storage = options.storage ?? getMemoryStorage<T>(queueName, options.capacity);
  }

  async start(): Promise<void> {
    if (this.isRunning) return;

    await this.storage.connect();

    this.isRunning = true;

    this.emit("worker:started", { 
        queueName: this.queueName,
        concurrency: this.concurrency
      });

    for (let i = 0; i < this.concurrency; i++) {
      void this.workerLoop(`${this.queueName}-worker-${i}`);
    }

    void this.delayedJobLoop();
    void this.recoveryLoop();
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

  private async workerLoop(workerId: string): Promise<void> {
    while (this.isRunning) {
      try {
        const claim = await this.storage.claim(this.queueName, { workerId: workerId, leaseMs: this.leaseMs, waitTimeoutMs: this.dequeueTimeoutMs });

        if(!claim) {
          continue;
        }
        this.activeWorkers++;
        this.updateMetrics();

        // const size = await this.storage.size(this.queueName);
        // this.metrics.updateQueueSize(size);

        // this.activeWorkers++;
        // this.updateMetrics();

        try {
          await this.processJob(claim, workerId);
        } finally {
          this.activeWorkers--;
          this.updateMetrics();
        }
      } catch (error) {
        this.emit("worker:error", { worker: workerId, error });
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  private async processJob(claim: Claim<T>, workerId: string): Promise<void> {
    const { job, claimToken } = claim;

    const startedAt = Date.now();
    // const startTime = Date.now();

    this.emit("job:processing", { job, worker: workerId });

    //job.status = "processing";
    const heartBeat = this.startHeartbeat(job.id, claimToken, workerId);

    try {
      await this.processor(job);
      // await this.storage.markCompleted(this.queueName, job.id);
      const complete = await this.storage.complete(this.queueName, job.id, claimToken); // complete() will also mark job as completed in storage, and handle edge cases like lease expiration or claim token mismatch
      if(!complete) {
        this.emit("job:lost-claim", { job, worker: workerId, phase: "complete" });
        return;
      }

      // job.status = "completed";
      this.metrics.incrementJobsCompleted();
      this.metrics.recordProcessingTime(Date.now() - startedAt);

      this.emit("job:completed", { 
          job : {...job, status: "completed" },
          duration: Date.now() - startedAt 
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);

      if (job.attempts < job.maxAttempts) {
        // job.status = "pending";

        const delayMs = Math.pow(2, job.attempts) * 1000;
        const executeAt = Date.now() + delayMs;

        // await this.storage.scheduleDelayed(this.queueName, job, executeAt);
        const retried = await  this.storage.retry(
          this.queueName,
          job.id,
          claimToken,
          executeAt,
          errorMessage
        )
        
        if(!retried) {
          this.emit("job:lost-claim", { job, workerId, phase: "retry", error: errorMessage });
          return;
        }

        this.metrics.incrementRetries();
        this.emit("job:retry", { 
          job: {
            ...job,
            status: "delayed",
            error: errorMessage,
            nextAttemptAt: new Date(executeAt) 
          },
          error: errorMessage, 
          nextAttemptAt: new Date(executeAt) 
        });

      } else {
        const failed = await this.storage.fail(
          this.queueName,
          job.id,
          claimToken,
          errorMessage
        )

        if(!failed) {
          this.emit("job:lost-claim", { job, workerId, phase: "fail", error: errorMessage })
          return;
        }

        // job.status = "failed";
        // await this.storage.markFailed(this.queueName, job.id, errorMessage);
        this.metrics.incrementJobsFailed();
        this.emit("job:failed", 
          { job: {...job, status: "failed", error: errorMessage },
          error: errorMessage 
        });
      }

    } finally {
      clearInterval(heartBeat);
    }
  }

  private startHeartbeat(jobId: string, claimToken: string, workerId: string): NodeJS.Timeout {
    return setInterval(async () => {
      void this.storage.
      renewLease(this.queueName, jobId, claimToken, this.leaseMs).
      then((renewed) => {
        if (!renewed) {
          this.emit("job:lost-claim", { jobId, workerId, phase: "heartbeat" });
        }
      }).catch((error) => {
        this.emit("worker:error", { workerId, error });
      });
    }, this.heartbeatIntervalMs);
  }

  private async delayedJobLoop(): Promise<void> {
    while (this.isRunning) {
      try {

        const promoted = await this.storage.promoteDelayedJobs(this.queueName, Date.now());

        if (promoted > 0) {
          this.emit("jobs:promoted", { count: promoted });

          // const size = await this.storage.size(this.queueName);
          // this.metrics.updateQueueSize(size);
        }
      } catch (error) {
        this.emit("worker:error", { workerId: "delayed-loop", error });
      }
      await new Promise((resolve) => setTimeout(resolve, this.delayedJobCheckIntervalMs));
    }
  }

  private async recoveryLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        const recovered = await this.storage.recoverExpiredJobs(this.queueName, Date.now());
        
        if (recovered > 0) {
          this.emit("jobs:recovered", { count: recovered });
        }
      } catch (error) {
        this.emit("worker:error", { workerId: "recovery-loop", error });
      }
      await new Promise((resolve) => setTimeout(resolve, this.recoveryIntervalMs));
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
