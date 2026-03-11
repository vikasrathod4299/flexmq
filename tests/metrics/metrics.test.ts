import { Metrics } from 'flexmq';

describe('Metrics', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it('should return a zeroed snapshot when no metrics have been recorded', () => {
    const metrics = new Metrics();
    const snapshot = metrics.getSnapshot();

    expect(snapshot.jobsAdded).toBe(0);
    expect(snapshot.jobsCompleted).toBe(0);
    expect(snapshot.jobsFailed).toBe(0);
    expect(snapshot.jobsDropped).toBe(0);
    expect(snapshot.totalRetries).toBe(0);
    expect(snapshot.avgProcessingTime).toBe(0);
    expect(snapshot.maxProcessingTime).toBe(0);
    expect(snapshot.minProcessingTime).toBe(0);
    expect(snapshot.p50ProcessingTime).toBe(0);
    expect(snapshot.p95ProcessingTime).toBe(0);
    expect(snapshot.p99ProcessingTime).toBe(0);
    expect(snapshot.successRate).toBe(0);
    expect(snapshot.errorRate).toBe(0);
    expect(snapshot.uptimeMs).toBeGreaterThanOrEqual(0);
  });

  it('should track counters, rates, worker stats, and processing percentiles', () => {
    const metrics = new Metrics();

    metrics.incrementJobsAdded();
    metrics.incrementJobsAdded();
    metrics.incrementJobsCompleted();
    metrics.incrementJobsFailed();
    metrics.incrementJobsDropped();
    metrics.incrementRetries();
    metrics.updateQueueSize(7);
    metrics.updateWorkerStats(2, 3);
    metrics.recordProcessingTime(100);
    metrics.recordProcessingTime(200);
    metrics.recordProcessingTime(300);

    const snapshot = metrics.getSnapshot();

    expect(snapshot.jobsAdded).toBe(2);
    expect(snapshot.jobsCompleted).toBe(1);
    expect(snapshot.jobsFailed).toBe(1);
    expect(snapshot.jobsDropped).toBe(1);
    expect(snapshot.totalRetries).toBe(1);
    expect(snapshot.queueSize).toBe(7);
    expect(snapshot.activeWorkers).toBe(2);
    expect(snapshot.idleWorkers).toBe(3);
    expect(snapshot.avgProcessingTime).toBe(200);
    expect(snapshot.maxProcessingTime).toBe(300);
    expect(snapshot.minProcessingTime).toBe(100);
    expect(snapshot.p50ProcessingTime).toBe(200);
    expect(snapshot.p95ProcessingTime).toBe(300);
    expect(snapshot.p99ProcessingTime).toBe(300);
    expect(snapshot.successRate).toBe(50);
    expect(snapshot.errorRate).toBe(50);
  });

  it('should return zero percentiles when no samples exist', () => {
    const metrics = new Metrics();

    expect(metrics.calculatePercentile(50)).toBe(0);
  });

  it('should keep only the latest 1000 processing samples for percentile calculations', () => {
    const metrics = new Metrics();

    for (let duration = 1; duration <= 1001; duration++) {
      metrics.recordProcessingTime(duration);
    }

    expect(metrics.calculatePercentile(0.1)).toBe(2);
    expect(metrics.getSnapshot().maxProcessingTime).toBe(1001);
    expect(metrics.getSnapshot().minProcessingTime).toBe(1);
  });

  it('should update throughput only after at least one second has elapsed', () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-03-08T00:00:00.000Z'));

    const metrics = new Metrics();

    jest.setSystemTime(new Date('2026-03-08T00:00:00.500Z'));
    metrics.incrementJobsCompleted();
    expect(metrics.getSnapshot().jobsPerSecond).toBe(0);

    jest.setSystemTime(new Date('2026-03-08T00:00:01.500Z'));
    metrics.incrementJobsCompleted();

    expect(metrics.getSnapshot().jobsPerSecond).toBeCloseTo(2 / 1.5, 5);
  });

  it('should reset counters and format Prometheus output from the current snapshot', () => {
    const metrics = new Metrics();

    metrics.incrementJobsAdded();
    metrics.incrementJobsCompleted();
    metrics.incrementJobsFailed();
    metrics.updateQueueSize(3);
    metrics.recordProcessingTime(100);
    metrics.recordProcessingTime(200);

    const prometheus = metrics.toPrometheusFormat();

    expect(prometheus).toContain('jobs_added_total 1');
    expect(prometheus).toContain('jobs_completed_total 1');
    expect(prometheus).toContain('jobs_failed_total 1');
    expect(prometheus).toContain('queue_size 3');
    expect(prometheus).toContain('processing_time_seconds{quantile="0.5"} 0.1');
    expect(prometheus).toContain('processing_time_seconds{quantile="0.95"} 0.2');
    expect(prometheus).toContain('processing_time_seconds{quantile="0.99"} 0.2');

    metrics.reset();

    const resetSnapshot = metrics.getSnapshot();
    expect(resetSnapshot.jobsAdded).toBe(0);
    expect(resetSnapshot.jobsCompleted).toBe(0);
    expect(resetSnapshot.jobsFailed).toBe(0);
    expect(resetSnapshot.jobsDropped).toBe(0);
    expect(resetSnapshot.totalRetries).toBe(0);
    expect(resetSnapshot.queueSize).toBe(0);
    expect(resetSnapshot.activeWorkers).toBe(0);
    expect(resetSnapshot.idleWorkers).toBe(0);
    expect(resetSnapshot.avgProcessingTime).toBe(0);
    expect(resetSnapshot.maxProcessingTime).toBe(0);
    expect(resetSnapshot.minProcessingTime).toBe(0);
    expect(metrics.toPrometheusFormat()).toContain('jobs_added_total 0');
  });
});