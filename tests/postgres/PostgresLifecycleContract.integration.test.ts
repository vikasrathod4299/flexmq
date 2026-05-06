/**
 * Lifecycle Contract Proof Tests for PostgresStorageAdapter.
 *
 * These tests prove that the Postgres adapter upholds every guarantee
 * defined in JOB_LIFECYCLE.md. Assertions are made against raw Postgres
 * tables so the adapter cannot mask its own bugs.
 *
 * Requires a running Postgres server:
 *   PGHOST=localhost PGPORT=5432 PGPASSWORD=nbcc_secret PGUSER=nbcc_user PGDATABASE=nbcc_db
 */
import { Client } from "pg";
import { type Job } from "flexmq";
import { PostgresStorageAdapter } from "@flexmq/postgres";

const POSTGRES_CONFIG = {
  host: process.env.PGHOST || "localhost",
  port: parseInt(process.env.PGPORT || "5432", 10),
  password: process.env.PGPASSWORD || "nbcc_secret",
  user: process.env.PGUSER || "nbcc_user",
  database: process.env.PGDATABASE || "nbcc_db",
  capacity: 100,
};

const Q = "pg-contract-test";

let adapter: PostgresStorageAdapter<{ email: string }>;
let db: Client;

const createJob = (
  id: string,
  overrides: Partial<Job<{ email: string }>> = {}
): Job<{ email: string }> => ({
  id,
  payload: { email: `${id}@test.com` },
  attempts: 0,
  maxAttempts: 3,
  status: "pending",
  nextAttemptAt: null,
  error: null,
  ...overrides,
});

async function cleanupTestRows() {
  await db.query(`DELETE FROM flexmq_job_events WHERE queue_name = $1`, [Q]);
  await db.query(`DELETE FROM flexmq_jobs WHERE queue_name = $1`, [Q]);
}

async function getJobRow(id: string) {
  const { rows } = await db.query(`SELECT * FROM flexmq_jobs WHERE queue_name = $1 AND id = $2`, [
    Q,
    id,
  ]);
  return rows[0] ?? null;
}

async function getPendingIds() {
  const { rows } = await db.query<{ id: string }>(
    `SELECT id FROM flexmq_jobs WHERE queue_name = $1 AND status = 'pending' ORDER BY created_at ASC`,
    [Q]
  );
  return rows.map((row) => row.id);
}

async function getProcessingIds() {
  const { rows } = await db.query<{ id: string }>(
    `SELECT id FROM flexmq_jobs WHERE queue_name = $1 AND status = 'processing' ORDER BY lease_until ASC NULLS LAST, created_at ASC`,
    [Q]
  );
  return rows.map((row) => row.id);
}

async function getDelayedRows() {
  const { rows } = await db.query<{ id: string; next_attempt_at: Date | null }>(
    `SELECT id, next_attempt_at FROM flexmq_jobs WHERE queue_name = $1 AND status = 'delayed' ORDER BY next_attempt_at ASC NULLS LAST`,
    [Q]
  );
  return rows;
}

beforeAll(async () => {
  db = new Client({
    host: POSTGRES_CONFIG.host,
    port: POSTGRES_CONFIG.port,
    password: POSTGRES_CONFIG.password,
    user: POSTGRES_CONFIG.user,
    database: POSTGRES_CONFIG.database,
  });
  await db.connect();

  adapter = new PostgresStorageAdapter(POSTGRES_CONFIG);
  await adapter.connect();
  await adapter.ensureSchema();
});

afterAll(async () => {
  await cleanupTestRows();
  await adapter.disconnect();
  await db.end();
});

beforeEach(async () => {
  await cleanupTestRows();
});

describe("Lifecycle Contract Proofs (JOB_LIFECYCLE.md)", () => {
  it("1. Concurrent claim race — exactly one worker wins", async () => {
    await adapter.enqueue(Q, createJob("race-1"));

    const claims = await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        adapter.claim(Q, { workerId: `w${i}`, leaseMs: 30000, waitTimeoutMs: 0 })
      )
    );

    const winners = claims.filter((claim) => claim !== null);
    expect(winners).toHaveLength(1);
    expect(await getPendingIds()).toEqual([]);
    expect(await getProcessingIds()).toEqual(["race-1"]);
  });

  it("2. Claimed job is invisible — second claim gets nothing", async () => {
    await adapter.enqueue(Q, createJob("invis-1"));
    await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    const second = await adapter.claim(Q, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
    expect(second).toBeNull();
    expect(await getPendingIds()).toEqual([]);
  });

  it("3. Complete is the ack boundary — status transitions durably", async () => {
    await adapter.enqueue(Q, createJob("ack-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    const before = await getJobRow("ack-1");
    expect(before.status).toBe("processing");
    expect(before.claim_token).toBe(claim!.claimToken);

    await adapter.complete(Q, "ack-1", claim!.claimToken);

    const after = await getJobRow("ack-1");
    expect(after.status).toBe("completed");
    expect(after.claim_token).toBeNull();
    expect(after.worker_id).toBeNull();
    expect(await getProcessingIds()).toEqual([]);
  });

  it("4. Crash-before-ack does not lose jobs — recovered after lease expiry", async () => {
    await adapter.enqueue(Q, createJob("crash-1"));
    await adapter.claim(Q, { workerId: "w1", leaseMs: 1, waitTimeoutMs: 0 });

    await new Promise((resolve) => setTimeout(resolve, 50));

    const before = await getJobRow("crash-1");
    expect(before.status).toBe("processing");

    await adapter.recoverExpiredJobs(Q, Date.now());

    const after = await getJobRow("crash-1");
    expect(after.status).toBe("pending");
    expect(after.claim_token).toBeNull();
    expect(await getPendingIds()).toEqual(["crash-1"]);
  });

  it("5. At-least-once delivery — job delivered twice after crash recovery", async () => {
    await adapter.enqueue(Q, createJob("atleast-1"));

    const c1 = await adapter.claim(Q, { workerId: "w1", leaseMs: 1, waitTimeoutMs: 0 });
    expect(c1).not.toBeNull();
    expect((await getJobRow("atleast-1")).attempts).toBe(1);

    await new Promise((resolve) => setTimeout(resolve, 50));
    await adapter.recoverExpiredJobs(Q, Date.now());

    const c2 = await adapter.claim(Q, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
    expect(c2).not.toBeNull();
    expect((await getJobRow("atleast-1")).attempts).toBe(2);
    expect(c2!.job.workerId).toBe("w2");
    expect(c2!.claimToken).not.toBe(c1!.claimToken);
  });

  it("6. Retry leaves processing cleanly — job in delayed, not processing", async () => {
    await adapter.enqueue(Q, createJob("retry-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
    const executeAt = Date.now() + 60000;

    await adapter.retry(Q, "retry-1", claim!.claimToken, executeAt, "transient");

    const job = await getJobRow("retry-1");
    const delayedRows = await getDelayedRows();

    expect(await getProcessingIds()).toEqual([]);
    expect(delayedRows.map((row) => row.id)).toEqual(["retry-1"]);
    expect(job.status).toBe("delayed");
    expect(job.claim_token).toBeNull();
    expect(job.worker_id).toBeNull();
    expect(job.lease_until).toBeNull();
    expect(job.error).toBe("transient");
    expect(new Date(job.next_attempt_at).getTime()).toBeGreaterThanOrEqual(executeAt - 5);
  });

  it("7. Delayed jobs are not claimable early", async () => {
    await adapter.enqueue(Q, createJob("early-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    await adapter.retry(Q, "early-1", claim!.claimToken, Date.now() + 600000);

    const promoted = await adapter.promoteDelayedJobs(Q);
    expect(promoted).toBe(0);
    expect(await getPendingIds()).toEqual([]);

    const claimAttempt = await adapter.claim(Q, {
      workerId: "w2",
      leaseMs: 30000,
      waitTimeoutMs: 0,
    });
    expect(claimAttempt).toBeNull();
    expect((await getDelayedRows()).map((row) => row.id)).toEqual(["early-1"]);
  });

  it("8. Delayed job promoted at correct time becomes claimable", async () => {
    await adapter.enqueue(Q, createJob("promo-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    await adapter.retry(Q, "promo-1", claim!.claimToken, Date.now() - 1000);

    const promoted = await adapter.promoteDelayedJobs(Q);
    expect(promoted).toBe(1);

    const job = await getJobRow("promo-1");
    expect(job.status).toBe("pending");
    expect(job.next_attempt_at).toBeNull();
    expect(await getPendingIds()).toEqual(["promo-1"]);

    const c2 = await adapter.claim(Q, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
    expect(c2).not.toBeNull();
    expect(c2!.job.id).toBe("promo-1");
  });

  it("9. Failed jobs are terminal and inspectable in Postgres", async () => {
    await adapter.enqueue(Q, createJob("term-1"));
    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    await adapter.fail(Q, "term-1", claim!.claimToken, "fatal: out of memory");

    const row = await getJobRow("term-1");
    expect(row.status).toBe("failed");
    expect(row.error).toBe("fatal: out of memory");
    expect(await getProcessingIds()).toEqual([]);
    expect(await getPendingIds()).toEqual([]);
  });

  it("10. Stale worker token rejected after recovery + new claim", async () => {
    await adapter.enqueue(Q, createJob("stale-1"));

    const c1 = await adapter.claim(Q, { workerId: "w1", leaseMs: 1, waitTimeoutMs: 0 });
    const staleToken = c1!.claimToken;

    await new Promise((resolve) => setTimeout(resolve, 50));
    await adapter.recoverExpiredJobs(Q, Date.now());

    const c2 = await adapter.claim(Q, { workerId: "w2", leaseMs: 30000, waitTimeoutMs: 0 });
    expect(c2).not.toBeNull();

    expect(await adapter.complete(Q, "stale-1", staleToken)).toBe(false);
    expect(await adapter.fail(Q, "stale-1", staleToken, "oops")).toBe(false);
    expect(await adapter.renewLease(Q, "stale-1", staleToken, 60000)).toBe(false);
    expect(await adapter.retry(Q, "stale-1", staleToken, Date.now() + 5000)).toBe(false);

    const row = await getJobRow("stale-1");
    expect(row.status).toBe("processing");
    expect(row.claim_token).toBe(c2!.claimToken);
    expect(row.worker_id).toBe("w2");
  });

  it("11. Recovery does not touch jobs with active leases", async () => {
    await adapter.enqueue(Q, createJob("active-1"));

    const claim = await adapter.claim(Q, { workerId: "w1", leaseMs: 60000, waitTimeoutMs: 0 });
    const before = await getJobRow("active-1");

    const recovered = await adapter.recoverExpiredJobs(Q, Date.now());
    expect(recovered).toBe(0);

    const after = await getJobRow("active-1");
    expect(after.status).toBe("processing");
    expect(after.claim_token).toBe(claim!.claimToken);
    expect(new Date(after.lease_until).getTime()).toBe(new Date(before.lease_until).getTime());
    expect(await getPendingIds()).toEqual([]);
  });

  it("12. FIFO ordering among pending jobs", async () => {
    await adapter.enqueue(Q, createJob("fifo-1"));
    await adapter.enqueue(Q, createJob("fifo-2"));
    await adapter.enqueue(Q, createJob("fifo-3"));

    expect(await getPendingIds()).toEqual(["fifo-1", "fifo-2", "fifo-3"]);

    const c1 = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
    const c2 = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });
    const c3 = await adapter.claim(Q, { workerId: "w1", leaseMs: 30000, waitTimeoutMs: 0 });

    expect(c1!.job.id).toBe("fifo-1");
    expect(c2!.job.id).toBe("fifo-2");
    expect(c3!.job.id).toBe("fifo-3");
  });
});
