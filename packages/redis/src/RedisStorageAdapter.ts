import type { Job, StorageAdapter, Claim, ClaimOptions } from "flexmq";
import type { RedisConfig } from "./RedisConfig";
import { randomUUID } from "node:crypto";
import Redis from "ioredis";
import * as path from "path";
import * as fs from "fs";

export class RedisStorageAdapter<T> implements StorageAdapter<T> {
  private client: Redis;
  private blockingClient: Redis;
  private config: RedisConfig;

  constructor(config: RedisConfig) {
    this.config = config;
    const redisOptions = {
      host: config.host,
      port: config.port,
      password: config.password,
      maxRetriesPerRequest: null,
    };
    this.client = new Redis(redisOptions);
    this.blockingClient = new Redis(redisOptions);
  }

  // ── Lua scripts ────────────────────────────────────────────────────
  private enqueueLua = fs.readFileSync(path.join(__dirname, "lua-scripts", "enqueue.lua"), "utf-8");
  private claimLua = fs.readFileSync(path.join(__dirname, "lua-scripts", "claim.lua"), "utf-8");
  private completeLua = fs.readFileSync(
    path.join(__dirname, "lua-scripts", "complete.lua"),
    "utf-8"
  );
  private failLua = fs.readFileSync(path.join(__dirname, "lua-scripts", "fail.lua"), "utf-8");
  private retryLua = fs.readFileSync(path.join(__dirname, "lua-scripts", "retry.lua"), "utf-8");
  private renewLeaseLua = fs.readFileSync(
    path.join(__dirname, "lua-scripts", "renew-lease.lua"),
    "utf-8"
  );
  private promoteDelayedLua = fs.readFileSync(
    path.join(__dirname, "lua-scripts", "promoteDelayed.lua"),
    "utf-8"
  );
  private recoverExpiredLua = fs.readFileSync(
    path.join(__dirname, "lua-scripts", "recover-expired.lua"),
    "utf-8"
  );

  // ── Key helpers ────────────────────────────────────────────────────
  private pendingKey(queueName: string): string {
    return `${queueName}:pending`;
  }
  private processingKey(queueName: string): string {
    return `${queueName}:processing`;
  }
  private delayedKey(queueName: string): string {
    return `${queueName}:delayed`;
  }
  private jobKey(queueName: string, id: string): string {
    return `${queueName}:job:${id}`;
  }
  private jobKeyPrefix(queueName: string): string {
    return `${queueName}:job:`;
  }

  // ── Connection lifecycle ───────────────────────────────────────────
  async connect(): Promise<void> {
    await this.client.ping();
    await this.blockingClient.ping();
  }

  async disconnect(): Promise<void> {
    await this.client.quit();
    await this.blockingClient.quit();
  }

  // ── Enqueue ────────────────────────────────────────────────────────
  async enqueue(queueName: string, job: Job<T>): Promise<boolean> {
    const now = Date.now();
    job.createdAt = job.createdAt || now;
    job.updatedAt = now;

    const jobData = JSON.stringify(job);

    const result = (await this.client.eval(
      this.enqueueLua,
      2,
      this.pendingKey(queueName),
      this.jobKey(queueName, job.id),
      this.config.capacity.toString(),
      job.id,
      jobData
    )) as number;

    return result === 1;
  }

  // ── Claim ─────────────────────────────────────
  async claim(queueName: string, options: ClaimOptions): Promise<Claim<T> | null> {
    const { workerId, leaseMs, waitTimeoutMs } = options;
    const claimToken = randomUUID();
    const timeout = waitTimeoutMs ?? 5000;

    // If waitTimeoutMs > 0, use BRPOP to block-wait for a job to appear,
    // then run the claim Lua to atomically set lease metadata.
    // If waitTimeoutMs === 0, try a direct non-blocking claim via Lua (which does RPOP internally).
    if (timeout > 0) {
      // BRPOP timeout is in seconds (minimum 1 second for Redis)
      const timeoutSec = Math.max(1, Math.ceil(timeout / 1000));
      const result = await this.blockingClient.brpop(this.pendingKey(queueName), timeoutSec);

      if (!result) return null;

      const [, jobId] = result;
      const now = Date.now();
      const leaseUntil = now + leaseMs;

      // We already popped the job from the list via BRPOP, so we need to
      // atomically set the claim metadata without re-popping.
      // Use a pipeline for atomicity of the metadata update.
      const jobKey = this.jobKey(queueName, jobId);

      const pipeline = this.client.multi();
      pipeline.hset(
        jobKey,
        "status",
        "processing",
        "workerId",
        workerId,
        "claimedAt",
        now.toString(),
        "leaseUntil",
        leaseUntil.toString(),
        "claimToken",
        claimToken,
        "updatedAt",
        now.toString(),
        "processingStartedAt",
        now.toString()
      );
      pipeline.hincrby(jobKey, "attempts", 1);
      pipeline.zadd(this.processingKey(queueName), leaseUntil.toString(), jobId);
      pipeline.hgetall(jobKey);
      const results = await pipeline.exec();

      if (!results) return null;

      // hgetall is the 4th command (index 3)
      const [err, data] = results[3] as [Error | null, Record<string, string>];
      if (err || !data) return null;

      const job = this.parseJobFromRedis(data);
      if (!job) return null;

      return { job, claimToken };
    }

    // Non-blocking: Lua script does RPOP + claim atomically
    const now = Date.now();
    const result = (await this.client.eval(
      this.claimLua,
      2,
      this.pendingKey(queueName),
      this.processingKey(queueName),
      this.jobKeyPrefix(queueName),
      workerId,
      leaseMs.toString(),
      now.toString(),
      claimToken
    )) as string | null;

    if (!result) return null;

    const job = this.parseJobFromRedisJson(result);
    if (!job) return null;

    return { job, claimToken };
  }

  // ── Renew lease ────────────────────────────────────────────────────
  async renewLease(
    queueName: string,
    jobId: string,
    claimToken: string,
    leaseMs: number
  ): Promise<boolean> {
    const now = Date.now();
    const result = (await this.client.eval(
      this.renewLeaseLua,
      1,
      this.processingKey(queueName),
      this.jobKey(queueName, jobId),
      jobId,
      claimToken,
      leaseMs.toString(),
      now.toString()
    )) as number;

    return result === 1;
  }

  // ── Complete ───────────────────────────────────────────────────────
  async complete(queueName: string, jobId: string, claimToken: string): Promise<boolean> {
    const now = Date.now();
    const result = (await this.client.eval(
      this.completeLua,
      1,
      this.processingKey(queueName),
      this.jobKey(queueName, jobId),
      jobId,
      claimToken,
      now.toString()
    )) as number;

    return result === 1;
  }

  // ── Fail ───────────────────────────────────────────────────────────
  async fail(
    queueName: string,
    jobId: string,
    claimToken: string,
    error?: string
  ): Promise<boolean> {
    const now = Date.now();
    const result = (await this.client.eval(
      this.failLua,
      1,
      this.processingKey(queueName),
      this.jobKey(queueName, jobId),
      jobId,
      claimToken,
      now.toString(),
      error ?? ""
    )) as number;

    return result === 1;
  }

  // ── Retry (move to delayed) ────────────────────────────────────────
  async retry(
    queueName: string,
    jobId: string,
    claimToken: string,
    executeAt: number,
    error?: string
  ): Promise<boolean> {
    const now = Date.now();
    const result = (await this.client.eval(
      this.retryLua,
      2,
      this.processingKey(queueName),
      this.delayedKey(queueName),
      this.jobKey(queueName, jobId),
      jobId,
      claimToken,
      now.toString(),
      executeAt.toString(),
      error ?? ""
    )) as number;

    return result === 1;
  }

  // ── Promote delayed jobs ───────────────────────────────────────────
  async promoteDelayedJobs(queueName: string, now: number = Date.now()): Promise<number> {
    return (await this.client.eval(
      this.promoteDelayedLua,
      2,
      this.delayedKey(queueName),
      this.pendingKey(queueName),
      this.jobKeyPrefix(queueName),
      now.toString()
    )) as number;
  }

  // ── Recover expired leases ─────────────────────────────────────────
  async recoverExpiredJobs(queueName: string, now: number): Promise<number> {
    return (await this.client.eval(
      this.recoverExpiredLua,
      2,
      this.processingKey(queueName),
      this.pendingKey(queueName),
      this.jobKeyPrefix(queueName),
      now.toString()
    )) as number;
  }

  // ── Query operations ───────────────────────────────────────────────
  async getJob(queueName: string, jobId: string): Promise<Job<T> | null> {
    const data = await this.client.hgetall(this.jobKey(queueName, jobId));
    return this.parseJobFromRedis(data);
  }

  async peek(queueName: string): Promise<Job<T> | null> {
    const jobId = await this.client.lindex(this.pendingKey(queueName), -1);
    if (!jobId) return null;
    return this.getJob(queueName, jobId);
  }

  async size(queueName: string): Promise<number> {
    return await this.client.llen(this.pendingKey(queueName));
  }

  async isFull(queueName: string): Promise<boolean> {
    return (await this.size(queueName)) >= this.config.capacity;
  }

  async isEmpty(queueName: string): Promise<boolean> {
    return (await this.size(queueName)) === 0;
  }

  async getProcessingJobs(queueName: string): Promise<string[]> {
    return this.client.zrange(this.processingKey(queueName), 0, -1);
  }

  // ── Parsing helpers ────────────────────────────────────────────────
  private parseJobFromRedis(data: Record<string, string>): Job<T> | null {
    if (!data || Object.keys(data).length === 0) return null;

    return {
      id: data["id"],
      payload: JSON.parse(data["payload"]) as T,
      attempts: parseInt(data["attempts"], 10),
      maxAttempts: parseInt(data["maxAttempts"], 10),
      status: data["status"] as Job<T>["status"],
      error: data["error"] || null,
      nextAttemptAt: data["nextAttemptAt"] ? new Date(parseInt(data["nextAttemptAt"], 10)) : null,
      createdAt: data["createdAt"] ? parseInt(data["createdAt"], 10) : undefined,
      updatedAt: data["updatedAt"] ? parseInt(data["updatedAt"], 10) : undefined,
      processingStartedAt: data["processingStartedAt"]
        ? parseInt(data["processingStartedAt"], 10)
        : undefined,
      workerId: data["workerId"] || undefined,
      claimedAt: data["claimedAt"] ? parseInt(data["claimedAt"], 10) : undefined,
      leaseUntil: data["leaseUntil"] ? parseInt(data["leaseUntil"], 10) : undefined,
      claimToken: data["claimToken"] || undefined,
    };
  }

  private parseJobFromRedisJson(json: string): Job<T> | null {
    try {
      const data = JSON.parse(json) as Record<string, string>;
      return this.parseJobFromRedis(data);
    } catch {
      return null;
    }
  }
}