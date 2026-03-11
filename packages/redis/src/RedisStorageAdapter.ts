import type { Job, StorageAdapter } from "flexmq";
import type { RedisConfig } from "./RedisConfig";
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

  private enqueueLua = fs.readFileSync(path.join(__dirname, "lua-scripts", "enqueue.lua"), "utf-8");
  private acquireJobLua = fs.readFileSync(
    path.join(__dirname, "lua-scripts", "acquire-job.lua"),
    "utf-8"
  );
  private promoteDelayedLua = fs.readFileSync(
    path.join(__dirname, "lua-scripts", "promoteDelayed.lua"),
    "utf-8"
  );
  private recoverStuckJobsLua = fs.readFileSync(
    path.join(__dirname, "lua-scripts", "recoverStuckJobs.lua"),
    "utf-8"
  );

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

  async connect(): Promise<void> {
    await this.client.ping();
    await this.blockingClient.ping();
  }

  async disconnect(): Promise<void> {
    await this.client.quit();
    await this.blockingClient.quit();
  }

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

  async dequeue(queueName: string, timeout: number = 5): Promise<Job<T> | null> {
    const result = await this.blockingClient.brpop(this.pendingKey(queueName), timeout);
    if (!result) return null;

    const [, jobId] = result;
    const now = Date.now();

    const jobData = (await this.client.eval(
      this.acquireJobLua,
      2,
      this.processingKey(queueName),
      this.jobKey(queueName, jobId),
      now.toString(),
      jobId
    )) as string | null;

    if (!jobData) return null;

    return JSON.parse(jobData) as Job<T>;
  }

  private parseJobFromRedis(data: Record<string, string>): Job<T> | null {
    if (!data || Object.keys(data).length === 0) return null;

    return {
      id: data["id"],
      payload: JSON.parse(data["payload"]) as T,
      attempts: parseInt(data["attempts"], 10),
      maxAttempts: parseInt(data["maxAttempts"], 10),
      status: data["status"] as Job<T>["status"],
      nextAttemptAt: data["nextAttemptAt"] ? new Date(parseInt(data["nextAttemptAt"], 10)) : null,
      createdAt: data["createdAt"] ? parseInt(data["createdAt"], 10) : undefined,
      updatedAt: data["updatedAt"] ? parseInt(data["updatedAt"], 10) : undefined,
      processingStartedAt: data["processingStartedAt"]
        ? parseInt(data["processingStartedAt"], 10)
        : undefined,
      workerId: data["workerId"] || undefined,
      error: data["error"] || null,
    };
  }

  async peek(queueName: string): Promise<Job<T> | null> {
    const jobId = await this.client.lindex(this.pendingKey(queueName), 0);
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

  async scheduleDelayed(queueName: string, job: Job<T>, executeAt: number): Promise<void> {
    job.updatedAt = Date.now();
    job.nextAttemptAt = new Date(executeAt);

    await this.client
      .multi()
      .hset(
        this.jobKey(queueName, job.id),
        "updatedAt",
        job.updatedAt.toString(),
        "nextAttemptAt",
        executeAt.toString()
      )
      .zadd(this.delayedKey(queueName), executeAt, job.id)
      .exec();
  }

  async markProcessing(queueName: string, jobId: string, workerId: string): Promise<void> {
    const now = Date.now();
    await this.client.zadd(this.processingKey(queueName), now, jobId);
    await this.client.hset(
      this.jobKey(queueName, jobId),
      "status",
      "processing",
      "processingStartedAt",
      now.toString(),
      "workerId",
      workerId,
      "updatedAt",
      now.toString()
    );
  }

  async markCompleted(queueName: string, jobId: string): Promise<void> {
    const now = Date.now();
    await this.client
      .multi()
      .zrem(this.processingKey(queueName), jobId)
      .hset(
        this.jobKey(queueName, jobId),
        "status",
        "completed",
        "processingStartedAt",
        "",
        "updatedAt",
        now.toString()
      )
      .expire(this.jobKey(queueName, jobId), 86400)
      .exec();
  }

  async markFailed(queueName: string, jobId: string, error: string = ""): Promise<void> {
    const now = Date.now();
    await this.client
      .multi()
      .zrem(this.processingKey(queueName), jobId)
      .hset(
        this.jobKey(queueName, jobId),
        "status",
        "failed",
        "processingStartedAt",
        "",
        "error",
        error || "",
        "updatedAt",
        now.toString()
      )
      .exec();
  }

  async promoteDelayedJobs(queueName: string): Promise<number> {
    const now = Date.now();
    return (await this.client.eval(
      this.promoteDelayedLua,
      2,
      this.delayedKey(queueName),
      this.pendingKey(queueName),
      `${queueName}:job:`,
      now
    )) as number;
  }

  async recoverStuckJobs(queueName: string, timeoutMs: number): Promise<number> {
    const now = Date.now();
    return (await this.client.eval(
      this.recoverStuckJobsLua,
      2,
      this.processingKey(queueName),
      this.pendingKey(queueName),
      `${queueName}:job:`,
      now,
      timeoutMs
    )) as number;
  }

  async updateJob(queueName: string, job: Job<T>): Promise<void> {
    job.updatedAt = Date.now();
    await this.client.hset(
      this.jobKey(queueName, job.id),
      "payload",
      JSON.stringify(job.payload),
      "attempts",
      job.attempts.toString(),
      "maxAttempts",
      job.maxAttempts.toString(),
      "status",
      job.status,
      "nextAttemptAt",
      job.nextAttemptAt ? job.nextAttemptAt.getTime().toString() : "",
      "updatedAt",
      job.updatedAt.toString(),
      "error",
      job.error || ""
    );
  }

  async getJob(queueName: string, jobId: string): Promise<Job<T> | null> {
    const data = await this.client.hgetall(this.jobKey(queueName, jobId));
    return this.parseJobFromRedis(data);
  }

  async getProcessingJobs(queueName: string): Promise<string[]> {
    return this.client.zrange(this.processingKey(queueName), 0, -1);
  }
}
