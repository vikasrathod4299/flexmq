import { Claim, ClaimOptions, Job, StorageAdapter } from "flexmq";
import {Pool , type PoolConfig} from "pg";
import { PostgresConfig } from "./PostgresConfig";


type JobRow<T> = {
    queue_name: string;
    id: string
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
    claim_at: Date | null;
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
        const poolConfig:PoolConfig = config.connectionString ? 
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
                claim_at TIMESTAMPTZ NULL,
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
            const { rows } = await client.query<{count: string}>(
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
        
    }

}