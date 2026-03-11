export interface MetricsSnapshot {
  jobsAdded: number;
  jobsCompleted: number;
  jobsFailed: number;
  jobsDropped: number;
  totalRetries: number;

  queueSize: number;
  activeWorkers: number;
  idleWorkers: number;

  avgProcessingTime: number;
  maxProcessingTime: number;
  minProcessingTime: number;

  p50ProcessingTime: number;
  p95ProcessingTime: number;
  p99ProcessingTime: number;

  jobsPerSecond: number;

  successRate: number; // percentage
  errorRate: number; // percentage

  uptimeMs: number;
}

export class Metrics {
  private jobsAdded: number = 0;
  private jobsCompleted: number = 0;
  private jobsFailed: number = 0;
  private jobsDropped: number = 0;
  private totalRetries: number = 0;

  private queueSize: number = 0;
  private activeWorkers: number = 0;
  private idleWorkers: number = 0;

  private readonly maxSamples: number = 1000;
  private processingTimes: number[] = [];
  private processingTimeIndex: number = 0;

  private totalProcessingTime: number = 0;
  private processedCount: number = 0;
  private maxProcessingTime: number = 0;
  private minProcessingTime: number = Infinity;

  private readonly startTime: number = Date.now();
  private lastThroughputCheck: number = Date.now();
  private jobsSinceLastCheck: number = 0;
  private currentThroughput: number = 0;

  incrementJobsAdded(): void {
    this.jobsAdded++;
  }

  incrementJobsCompleted(): void {
    this.jobsCompleted++;
    this.jobsSinceLastCheck++;
    this.updateThroughput();
  }

  incrementJobsFailed(): void {
    this.jobsFailed++;
  }

  incrementJobsDropped(): void {
    this.jobsDropped++;
  }

  incrementRetries(): void {
    this.totalRetries++;
  }

  recordProcessingTime(durationMs: number): void {
    if (this.processingTimes.length >= this.maxSamples) {
      this.processingTimes.shift();
    }
    this.processingTimes.push(durationMs);

    this.totalProcessingTime += durationMs;
    this.processedCount++;
    this.maxProcessingTime = Math.max(this.maxProcessingTime, durationMs);
    this.minProcessingTime = Math.min(this.minProcessingTime, durationMs);
  }

  updateQueueSize(size: number): void {
    this.queueSize = size;
  }

  updateWorkerStats(active: number, idle: number): void {
    this.activeWorkers = active;
    this.idleWorkers = idle;
  }

  updateThroughput(): void {
    const now = Date.now();
    const elapsed = now - this.lastThroughputCheck;

    if (elapsed >= 1000) {
      this.currentThroughput = (this.jobsSinceLastCheck / elapsed) * 1000;
      this.lastThroughputCheck = now;
      this.jobsSinceLastCheck = 0;
    }
  }

  calculatePercentile(percentile: number): number {
    if (this.processingTimes.length === 0) return 0;

    const sorted = [...this.processingTimes].sort((a, b) => a - b);
    const index = Math.ceil((percentile / 100) * sorted.length) - 1;
    return sorted[index];
  }

  getSnapshot(): MetricsSnapshot {
    const totaljobs = this.jobsCompleted + this.jobsFailed;

    return {
      jobsAdded: this.jobsAdded,
      jobsCompleted: this.jobsCompleted,
      jobsFailed: this.jobsFailed,
      jobsDropped: this.jobsDropped,
      totalRetries: this.totalRetries,

      queueSize: this.queueSize,
      activeWorkers: this.activeWorkers,
      idleWorkers: this.idleWorkers,

      avgProcessingTime:
        this.processedCount > 0 ? this.totalProcessingTime / this.processedCount : 0,
      maxProcessingTime: this.maxProcessingTime,
      minProcessingTime: this.minProcessingTime === Infinity ? 0 : this.minProcessingTime,

      p50ProcessingTime: this.calculatePercentile(50),
      p95ProcessingTime: this.calculatePercentile(95),
      p99ProcessingTime: this.calculatePercentile(99),

      jobsPerSecond: this.currentThroughput,

      successRate: totaljobs > 0 ? (this.jobsCompleted / totaljobs) * 100 : 0,
      errorRate: totaljobs > 0 ? (this.jobsFailed / totaljobs) * 100 : 0,

      uptimeMs: Date.now() - this.startTime,
    };
  }

  reset(): void {
    this.jobsAdded = 0;
    this.jobsCompleted = 0;
    this.jobsFailed = 0;
    this.jobsDropped = 0;
    this.totalRetries = 0;

    this.queueSize = 0;
    this.activeWorkers = 0;
    this.idleWorkers = 0;

    this.processingTimes = [];
    this.processingTimeIndex = 0;

    this.totalProcessingTime = 0;
    this.processedCount = 0;
    this.maxProcessingTime = 0;
    this.minProcessingTime = Infinity;

    this.jobsSinceLastCheck = 0;
    this.currentThroughput = 0;
  }

  toPrometheusFormat(): string {
    const snapshot = this.getSnapshot();
    return `
      # HELP jobs_added_total Total jobs added to queue
      # TYPE jobs_added_total counter
      jobs_added_total ${snapshot.jobsAdded}

      # HELP jobs_completed_total Total jobs completed successfully
      # TYPE jobs_completed_total counter
      jobs_completed_total ${snapshot.jobsCompleted}

      # HELP jobs_failed_total Total jobs failed
      # TYPE jobs_failed_total counter
      jobs_failed_total ${snapshot.jobsFailed}

      # HELP queue_size Current queue size
      # TYPE queue_size gauge
      queue_size ${snapshot.queueSize}

      # HELP processing_time_seconds Job processing time
      # TYPE processing_time_seconds summary
      processing_time_seconds{quantile="0.5"} ${snapshot.p50ProcessingTime / 1000}
      processing_time_seconds{quantile="0.95"} ${snapshot.p95ProcessingTime / 1000}
      processing_time_seconds{quantile="0.99"} ${snapshot.p99ProcessingTime / 1000}
    `.trim();
  }
}

export default Metrics;
