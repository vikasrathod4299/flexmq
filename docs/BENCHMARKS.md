# Benchmarks

This repository includes a minimal benchmark harness for publishing reproducible throughput and latency numbers.

## Goals

- compare in-memory and Redis adapter performance
- measure end-to-end queue latency, not just isolated method calls
- publish transparent results with environment details

## Included scenarios

- `memory-end-to-end`
- `redis-end-to-end`

Both scenarios:

- enqueue a fixed number of jobs
- run a no-op worker processor
- measure enqueue-to-complete latency
- report throughput and latency percentiles as JSON

## Commands

From the repo root:

```bash
npm run bench:memory -- --jobs=2000 --concurrency=4
npm run bench:redis -- --jobs=2000 --concurrency=4 --host=localhost --port=6379 --password=your_password
```

Defaults:

- `jobs=2000`
- `concurrency=4`
- Redis host/port/password fall back to `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`

## Output format

Each benchmark prints JSON like:

```json
{
  "scenario": "redis-end-to-end",
  "adapter": "redis",
  "jobs": 2000,
  "elapsedMs": 412.5,
  "throughputPerSec": 4848.48,
  "latencyMs": {
    "p50": 1.2,
    "p95": 4.6,
    "p99": 9.1
  },
  "environment": {
    "node": "v22.x",
    "platform": "linux-x64",
    "cpus": 8,
    "hostname": "bench-host"
  }
}
```