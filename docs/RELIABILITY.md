# Reliability and Stress Testing

This repository includes a minimal reliability harness to demonstrate crash recovery and blocked-producer progress under Redis.

## Included scenarios

### `redis-crash-recovery`

Simulates workers claiming jobs and then disappearing before ack.

What it verifies:

- jobs can be claimed with short leases
- expired leases are recovered back to pending
- recovered jobs can be claimed again and completed
- no job is lost in this scenario

Command:

```bash
npm run stress:recovery -- --jobs=200 --host=localhost --port=6379 --password=your_password
```

### `redis-block-producer-progress`

Fills a small-capacity queue with many producers using `BLOCK_PRODUCER`, then frees pending capacity from another Redis client.

What it verifies:

- blocked producers eventually resume
- distributed capacity wakeups work across processes
- pending-capacity semantics continue to make progress under contention

Command:

```bash
npm run stress:block-producer -- --producers=20 --capacity=3 --host=localhost --port=6379 --password=your_password
```

## Output format

Each scenario prints JSON with:

- summary counters
- invariant checks
- environment metadata

Example shape:

```json
{
  "scenario": "redis-crash-recovery",
  "adapter": "redis",
  "durationMs": 211.2,
  "summary": {
    "jobs": 200,
    "claimed": 200,
    "recovered": 200,
    "completed": 200
  },
  "invariants": [
    {
      "name": "all recovered jobs completed",
      "passed": true,
      "detail": "completed=200 jobs=200"
    }
  ]
}
```

## What these tests do and do not prove

They help demonstrate:

- lease expiry recovery works
- blocked producers do not deadlock under Redis contention
- the queue satisfies at-least-once delivery expectations under induced failure

They do not prove:

- exactly-once delivery
- universal production SLOs
- behavior under every network or Redis failure mode