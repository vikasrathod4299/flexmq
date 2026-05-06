import { Claim, ClaimOptions, Job, StorageAdapter } from "flexmq";
import { Pool, type PoolConfig } from "pg";
import { PostgresConfig } from "./PostgresConfig";
import { randomUUID } from "node:crypto";


type JobRow<T> = {
  queue_name: string;
  id: string;
  job_id: string;
  payload: unknown;
  status: Job<T>["status"];
  attempts: number;
  max_attempts: number;
  error: string | null;
  next_attempt_at: Date | null;
  created_at: Date;
  updated_at: Date;
  worker_id: string | null;
  claimed_at: Date | null;
  lease_until: Date | null;
  claim_token: string | null;
  processing_started_at: Date | null;
}

type JobEventRow = {
  event_id: string;
}

export class PostgresStorageAdapter<T> implements StorageAdapter<T> {
  private pool: Pool;
  private config: PostgresConfig;

  constructor(config: PostgresConfig) {
    this.config = config;
    const poolConfig: PoolConfig = config.connectionString ?
      { connectionString: config.connectionString, ssl: config.ssl as PoolConfig["ssl"] }
      :
      {
        host: config.host,
        port: config.port,
        user: config.user,
        password: config.password,
        database: config.database,
        ssl: config.ssl as PoolConfig["ssl"]
      };
    this.pool = new Pool(poolConfig);
  }

  async connect(): Promise<void> {
    await this.pool.query("SELECT 1");
  }

  async disconnect(): Promise<void> {
    await this.pool.end();
  }

  async ensureSchema(): Promise<void> {
    await this.pool.query(`
            CREATE TABLE IF NOT EXISTS flexmq_jobs (
                queue_name TEXT NOT NULL,
                id UUID DEFAULT gen_random_uuid(),
                job_id TEXT NOT NULL,
                payload JSONB NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,
                error TEXT NULL,
                next_attempt_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                worker_id TEXT NULL,
                claimed_at TIMESTAMPTZ NULL,
                lease_until TIMESTAMPTZ NULL,
                claim_token TEXT NULL,
                processing_started_at TIMESTAMPTZ NULL,
                PRIMARY KEY (queue_name, id)
            );
        `);

    await this.pool.query(`
            CREATE TABLE IF NOT EXISTS flexmq_job_events (
                event_id BIGSERIAL PRIMARY KEY,
                queue_name TEXT NOT NULL,
                job_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                status TEXT NOT NULL,
                worker_id TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

    await this.pool.query(`
            CREATE INDEX IF NOT EXISTS flexmq_jobs_pending_idx ON flexmq_jobs (queue_name, status, created_at)
        `);
    await this.pool.query(`
            CREATE INDEX IF NOT EXISTS flexmq_jobs_delayed_idx ON flexmq_jobs (queue_name, status, next_attempt_at)
        `);
    await this.pool.query(`
            CREATE INDEX IF NOT EXISTS flexmq_jobs_processing_idx ON flexmq_jobs (queue_name, status, lease_until)
        `);
    await this.pool.query(`
            CREATE INDEX IF NOT EXISTS flexmq_job_events_queue_idx ON flexmq_job_events (queue_name, event_id)
        `);
    await this.pool.query(`
            CREATE INDEX IF NOT EXISTS flexmq_job_events_job_idx ON flexmq_job_events (queue_name, job_id, event_id)
        `);

  }

  async enqueue(queueName: string, job: Job<T>): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const { rows } = await client.query<{ count: string }>(
        `SELECT COUNT(*)::text AS count FROM flexmq_jobs WHERE queue_name = $1 AND status = 'pending'`,
        [queueName]
      );

      const pendingCount = Number(rows[0].count ?? "0");
      if (pendingCount >= this.config.capacity) {
        await client.query("ROLLBACK");
        return false;
      }

      const now = new Date();
      const createdAt = job.createdAt ? new Date(job.createdAt) : now;
      const updatedAt = new Date();

      await client.query(
        `
                    INSERT INTO flexmq_jobs (
                        queue_name, id, payload, status, attempts, max_attempts, error, next_attempt_at, created_at, updated_at,
                        worker_id, claim_at, lease_until, claim_token, processing_started_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                `,
        [
          queueName,
          job.id,
          JSON.stringify(job.payload),
          "pending",
          job.attempts,
          job.maxAttempts,
          job.error,
          job.nextAttemptAt,
          createdAt,
          updatedAt,
          null,
          null,
          null,
          null,
          null
        ]
      );

      await client.query("COMMIT");
      return true
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }

  }

  async claim(queueName: string, options: ClaimOptions): Promise<Claim<T> | null> {
    const timeoutMs = options.waitTimeoutMs ?? 5000;
    const deadline = Date.now() + timeoutMs

    do {
      const claim = await this.tryClaim(queueName, options);

      if (claim) {
        return claim;
      }

      if (timeoutMs === 0) {
        return null;
      }

      const remainingMs = deadline - Date.now();

      if (remainingMs <= 0) {
        return null;
      }

      await this.waitForWakeHint(queueName, remainingMs);
    } while (Date.now() < deadline);

    return null

  }

  async renewLease(queueName: string, jobId: string, claimToken: string, leaseMs: number): Promise<boolean> {
    const now = new Date();
    const leaseUntil = new Date(now.getTime() + leaseMs);
    const result = await this.pool.query(
      `
        UPDATE flexmq_jobs
        SET lease_until = $4,
            updated_at = $5
        WHERE queue_name = $1 AND id = $2 AND status = 'processing' AND claim_token = $3
      `,
      [queueName, jobId, claimToken, leaseUntil, now]
    );

    return result.rowCount === 1;
  }

  async complete(queueName: string, jobId: string, claimToken: string): Promise<boolean> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");
      const now = new Date();
      const result = await client.query(
        `
        UPDATE flexmq_jobs
        SET status = 'completed',
            updated_at = $4
            worker_id = NULL,
            claimed_at = NULL,
            lease_until = NULL,
            claim_token = NULL,
        WHERE queue_name = $1 AND id = $2 AND status = 'processing' AND claim_token = $3
      `,
        [queueName, jobId, claimToken, now]
      );

      if (result.rowCount !== 1) {
        await client.query("ROLLBACK");
        return false;
      }

      await client.query(`
            INSERT INTO flexmq_job_events (queue_name, job_id, event_type, status, worker_id)
            VALUES ($1, $2, 'job_terminal', 'completed', NULL)
        `, [queueName, jobId]);

      await client.query(`NOTIFY flexmq_events, $1`, [queueName]);

      await client.query("COMMIT");
      return true;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async fail(queueName: string, jobId: string, claimToken: string, error?: string): Promise<boolean> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");


      const now = new Date();
      const result = await client.query(
        `
        UPDATE flexmq_jobs
        SET status = 'failed',
            error = $4,
            updated_at = $5,
            worker_id = NULL,
            claimed_at = NULL,
            lease_until = NULL,
            claim_token = NULL
        WHERE queue_name = $1 AND id = $2 AND status = 'processing' AND claim_token = $3
      `,
        [queueName, jobId, claimToken, error ?? "", now]
      );

      if (result.rowCount !== 1) {
        await client.query("ROLLBACK");
        return false;
      }

      await client.query(`
            INSERT INTO flexmq_job_events (queue_name, job_id, event_type, status, worker_id)
            VALUES ($1, $2, 'job_terminal', 'failed', NULL)
        `, [queueName, jobId]);

      await client.query(`NOTIFY flexmq_events, $1`, [queueName]);

      await client.query("COMMIT");
      return true;

    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async retry(queueName: string, jobId: string, claimToken: string, executeAt: number, error?: string): Promise<boolean> {
    const now = new Date();

    const result = await this.pool.query(`
        UPDATE flexmq_jobs
        SET status = 'delayed',
            error = $4,
            next_attempt_at = $5,
            updated_at = $6,
            worker_id = NULL,
            claimed_at = NULL,
            lease_until = NULL,
            claim_token = NULL
        WHERE queue_name = $1 AND id = $2 AND status = 'processing' AND claim_token = $3
        `, [queueName, jobId, claimToken, error ?? "", new Date(executeAt), now]
    )

    return result.rowCount === 1;
  }

  async promoteDelayedJobs(queueName: string, now?: number): Promise<number> {
    const result = await this.pool.query(
      `
        UPDATE flexmq_jobs
        SET status = 'pending',
            next_attempt_at = NULL,
            updated_at = $2
        WHERE queue_name = $1 AND status = 'delayed' AND next_attempt_at IS NOT NULL AND next_attempt_at <= $2
        `,
      [queueName, now ? new Date(now) : new Date()]
    )

    if ((result.rowCount ?? 0) > 0) {
      await this.pool.query(`NOTIFY flexmq_events, $1`, [queueName]);
    }

    return result.rowCount ?? 0;
  }

  async recoverExpiredJobs(queueName: string, now: number): Promise<number> {
    const result = await this.pool.query(`
        UPDATE flexmq_jobs
        SET status = 'pending',
            updated_at = $2,
            worker_id = NULL,
            claimed_at = NULL,
            lease_until = NULL,
            claim_token = NULL
        WHERE queue_name = $1 AND status = 'processing' AND lease_until IS NOT NULL AND lease_until <= $2
        `, [queueName, new Date(now)]
    );

    if ((result.rowCount ?? 0) > 0) {
      await this.pool.query(`NOTIFY flexmq_events, $1`, [queueName]);
    }

    return result.rowCount ?? 0;
  }

  async size(queueName: string): Promise<number> {
    const { rows } = await this.pool.query<{ count: string }>(
      `
        SELECT COUNT(*)::text AS count FROM flexmq_jobs WHERE queue_name = $1 AND status = 'pending'
      `,
      [queueName]
    );

    return Number(rows[0]?.count ?? 0)

  }

  async isFull(queueName: string): Promise<boolean> {
    return (await this.size(queueName)) >= this.config.capacity;
  }

  async waitForCapacity(queueName: string, timeoutMs: number): Promise<boolean> {
    if (!(await this.isFull(queueName))) {
      return true
    }

    if (timeoutMs <= 0) {
      return false
    }

    await this.waitForWakeHint(queueName, timeoutMs);
    return !(await this.isFull(queueName));
  }

  async waitForTerminalState(queueName: string, jobId: string, timeoutMs: number): Promise<Job<T> | null> {
    const current = await this.getJob(queueName, jobId);

    if (!current) {
      return null;
    }

    if (this.isTerminamJob(current)) {
      return current;
    }

    if (timeoutMs <= 0) {
      return null;
    }

    await this.waitForWakeHint(queueName, timeoutMs);

    const updated = await this.getJob(queueName, jobId);

    if (!updated) {
      return null;
    }

    return this.isTerminamJob(updated) ? updated : null;
  }

  async isEmpty(queueName: string): Promise<boolean> {
    return (await this.size(queueName)) === 0;
  }

  async peek(queueName: string): Promise<Job<T> | null> {
    const { rows } = await this.pool.query<JobRow<T>>(
      `SELECT *
       FROM flexmq_jobs
       WHERE queue_name = $1 AND status = 'pending'
       ORDER BY created_at ASC
       LIMIT 1`
      , [queueName]
    )

    const row = rows[0];

    return row ? this.mapRowToJob(row) : null;

  }

  async getProcessingJobs(queueName: string): Promise<string[]> {
    const { rows } = await this.pool.query<{ id: string }>(
      `SELECT id FROM flexmq_jobs WHERE queue_name = $1 AND status = 'processing'
       ORDER BY lease_until ASC NULLS LAST, created_at ASC`
      , [queueName]
    )
    return rows.map(r => r.id);
  }

  async getJob(queueName: string, jobId: string): Promise<Job<T> | null> {
    const { rows } = await this.pool.query<JobRow<T>>(
      `SELECT * FROM flexmq_jobs WHERE queue_name = $1 AND id = $2`,
      [queueName, jobId]
    )
    const row = rows[0];
    return row ? this.mapRowToJob(row) : null;
  }

  private async tryClaim(queueName: string, options: ClaimOptions): Promise<Claim<T> | null> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      const selected = await client.query<{ id: string }>(
        `
          SELECT id FROM flexmq_jobs
          WHERE queue_name = $1
            AND status = 'pending'
            AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
          ORDER BY created_at ASC
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        `,
        [queueName]
      );
      const row = selected.rows[0];
      if (!row) {
        await client.query("ROLLBACK");
        return null;
      }
      const now = new Date();
      const claimToken = randomUUID();
      const leaseUntil = new Date(now.getTime() + options.leaseMs);

      const updated = await client.query<JobRow<T>>(
        `
          UPDATE flexmq_jobs
          SET status = 'processing',
              attempts = attempts + 1,
              worker_id = $3, 
              claim_at = $4,
              lease_until = $5,
              claim_token = $6,
              updated_at = $4,
              processing_started_at = $4,
              next_attempt_at = NULL
          WHERE queue_name = $1 and id = $2
          RETURNING *
        `,
        [queueName, row.id, options.workerId, now, leaseUntil, claimToken]
      );

      await client.query(
        `
            INSERT INTO flexmq_job_events (queue_name, job_id, event_type, status, worker_id)
            VALUES ($1, $2, 'capacity_freed', 'processing', $3)
          `,
        [queueName, row.id, options.workerId]
      );
      await client.query(`NOTIFY flexmq_events, $1`, [queueName]);
      await client.query("COMMIT");

      const jobRow = updated.rows[0];
      if (!jobRow) {
        return null;
      }

      return {
        job: this.mapRowToJob(jobRow),
        claimToken
      }
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  private async waitForWakeHint(queueName: string, timeoutMs: number): Promise<void> {
    const client = await this.pool.connect();
    try {
      const channel = "flexmq_events";
      await client.query(`LISTEN ${channel}`);

      let resolved = false

      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          if (resolved) return;
          resolved = true;
          cleanup();
          resolve();
        }, timeoutMs);

        const onNotification = (msg: { channel?: string; payload?: string }) => {
          if (msg.channel !== channel) return;
          if (msg.payload !== queueName) return;
          if (resolved) return;

          resolved = true;
          cleanup();
          resolve();
        }
        const cleanup = () => {
          clearTimeout(timer);
          client.off("notification", onNotification);
        }
        client.on("notification", onNotification);
      });
    } finally {
      try {
        await client.query("UNLISTEN flexmq_events");
      } finally {
        client.release();
      }
    }
  }

  private isTerminamJob(job: Job<T>): boolean {
    return job.status === "completed" || job.status === "failed";
  }

  private mapRowToJob(row: JobRow<T>): Job<T> {
    return {
      id: row.id,
      payload: row.payload as T,
      status: row.status,
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      error: row.error,
      nextAttemptAt: row.next_attempt_at,
      createdAt: row.created_at.getTime(),
      updatedAt: row.updated_at.getTime(),
      workerId: row.worker_id ?? undefined,
      claimedAt: row.claimed_at ? row.claimed_at.getTime() : undefined,
      leaseUntil: row.lease_until ? row.lease_until.getTime() : undefined,
      claimToken: row.claim_token ?? undefined,
      processingStartedAt: row.processing_started_at ? row.processing_started_at.getTime() : undefined
    }
  }

}
