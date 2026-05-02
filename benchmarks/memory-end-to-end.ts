import { Queue, Worker } from "../packages/core/src";
import {
  environmentInfo,
  latencySummary,
  nowMs,
  parseIntArg,
  printJsonResult,
  throughputPerSec,
} from "../scripts/helpers";

type Payload = { id: number; body: string };

async function main(): Promise<void> {
  const jobs = parseIntArg("jobs", 2000);
  const concurrency = parseIntArg("concurrency", 4);
  const queueName = `bench-memory-${Date.now()}`;

  const queue = new Queue<Payload>(queueName, { capacity: Math.max(jobs + 10, 1000) });
  const completionLatencies: number[] = [];
  const createdAtByJobId = new Map<string, number>();
  let completed = 0;

  const worker = new Worker<Payload>(queueName, {
    storage: queue.getStorage(),
    concurrency,
    processor: async () => undefined,
  });

  worker.on("job:completed", ({ job }) => {
    const createdAt = createdAtByJobId.get(job.id);
    if (createdAt !== undefined) {
      completionLatencies.push(nowMs() - createdAt);
    }
    completed += 1;
  });

  const startedAt = nowMs();

  try {
    await queue.connect();
    await worker.start();

    for (let i = 0; i < jobs; i += 1) {
      const job = await queue.add({ id: i, body: "x".repeat(128) }, { maxAttempts: 1 });
      createdAtByJobId.set(job.id, nowMs());
    }

    while (completed < jobs) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    const elapsedMs = nowMs() - startedAt;
    printJsonResult({
      scenario: "memory-end-to-end",
      adapter: "memory",
      jobs,
      elapsedMs,
      throughputPerSec: throughputPerSec(jobs, elapsedMs),
      latencyMs: latencySummary(completionLatencies),
      notes: ["Processor is a no-op", "Measures enqueue-to-complete latency"],
      environment: environmentInfo(),
    });
  } finally {
    await worker.stop();
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
