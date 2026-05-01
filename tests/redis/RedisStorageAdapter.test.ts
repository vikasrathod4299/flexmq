const mockRedisInstances: MockRedisClient[] = [];

type MockRedisMulti = {
  hset: jest.Mock;
  hincrby: jest.Mock;
  zadd: jest.Mock;
  zrem: jest.Mock;
  expire: jest.Mock;
  hgetall: jest.Mock;
  exec: jest.Mock;
};

type MockRedisClient = {
  options: unknown;
  ping: jest.Mock;
  quit: jest.Mock;
  eval: jest.Mock;
  brpop: jest.Mock;
  rpop: jest.Mock;
  lindex: jest.Mock;
  hgetall: jest.Mock;
  llen: jest.Mock;
  multi: jest.Mock;
  zadd: jest.Mock;
  hset: jest.Mock;
  zrange: jest.Mock;
  xadd: jest.Mock;
  xrevrange: jest.Mock;
  xread: jest.Mock;
  __multi: MockRedisMulti;
};

const mockCreateRedisClient = (options: unknown): MockRedisClient => {
  const multi: MockRedisMulti = {
    hset: jest.fn().mockReturnThis(),
    hincrby: jest.fn().mockReturnThis(),
    zadd: jest.fn().mockReturnThis(),
    zrem: jest.fn().mockReturnThis(),
    expire: jest.fn().mockReturnThis(),
    hgetall: jest.fn().mockReturnThis(),
    exec: jest.fn().mockResolvedValue([]),
  };

  const client: MockRedisClient = {
    options,
    ping: jest.fn().mockResolvedValue("PONG"),
    quit: jest.fn().mockResolvedValue("OK"),
    eval: jest.fn(),
    brpop: jest.fn(),
    rpop: jest.fn(),
    lindex: jest.fn(),
    hgetall: jest.fn(),
    llen: jest.fn(),
    multi: jest.fn(() => multi),
    zadd: jest.fn().mockResolvedValue(1),
    hset: jest.fn().mockResolvedValue(1),
    zrange: jest.fn().mockResolvedValue([]),
    xadd: jest.fn().mockResolvedValue("1-0"),
    xrevrange: jest.fn().mockResolvedValue([]),
    xread: jest.fn().mockResolvedValue(null),
    __multi: multi,
  };

  mockRedisInstances.push(client);
  return client;
};

jest.mock("ioredis", () => ({
  __esModule: true,
  default: jest.fn().mockImplementation((options) => mockCreateRedisClient(options)),
}));

import Redis from "ioredis";
import { type Job } from "flexmq";
import { RedisStorageAdapter } from "@flexmq/redis";

const createJob = (id: string): Job<{ email: string }> => ({
  id,
  payload: { email: `${id}@test.com` },
  attempts: 0,
  maxAttempts: 5,
  status: "pending",
  nextAttemptAt: null,
  error: null,
});

const createRedisJobHash = (
  overrides: Partial<Record<string, string>> = {}
): Record<string, string> => ({
  id: "job-1",
  payload: JSON.stringify({ email: "job-1@test.com" }),
  attempts: "1",
  maxAttempts: "5",
  status: "processing",
  nextAttemptAt: "",
  createdAt: "1741391000000",
  updatedAt: "1741391500000",
  processingStartedAt: "1741391600000",
  workerId: "worker-1",
  claimedAt: "1741391600000",
  leaseUntil: "1741421600000",
  claimToken: "test-token-123",
  error: "",
  ...overrides,
});

describe("RedisStorageAdapter", () => {
  const config = {
    host: "localhost",
    port: 6379,
    password: "secret",
    capacity: 10,
  };

  const getClients = () => {
    const [client, blockingClient, streamClient] = mockRedisInstances;
    return { client, blockingClient, streamClient };
  };

  beforeEach(() => {
    mockRedisInstances.length = 0;
    jest.clearAllMocks();
  });

  describe("constructor", () => {
    it("should create three redis clients with the expected options", () => {
      new RedisStorageAdapter(config);

      const RedisMock = Redis as unknown as jest.Mock;
      expect(RedisMock).toHaveBeenCalledTimes(3);
      expect(RedisMock).toHaveBeenNthCalledWith(
        1,
        expect.objectContaining({
          host: "localhost",
          port: 6379,
          password: "secret",
          maxRetriesPerRequest: null,
        })
      );
    });
  });

  describe("connect / disconnect", () => {
    it("should connect and disconnect all three redis clients", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client, blockingClient, streamClient } = getClients();

      await adapter.connect();
      await adapter.disconnect();

      expect(client.ping).toHaveBeenCalledTimes(1);
      expect(blockingClient.ping).toHaveBeenCalledTimes(1);
      expect(streamClient.ping).toHaveBeenCalledTimes(1);
      expect(client.quit).toHaveBeenCalledTimes(1);
      expect(blockingClient.quit).toHaveBeenCalledTimes(1);
      expect(streamClient.quit).toHaveBeenCalledTimes(1);
    });
  });

  describe("enqueue", () => {
    it("should enqueue jobs through the Lua script and return success/failure", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();
      const job = createJob("job-1");

      client.eval.mockResolvedValueOnce(1).mockResolvedValueOnce(0);

      await expect(adapter.enqueue("emails", job)).resolves.toBe(true);
      await expect(adapter.enqueue("emails", createJob("job-2"))).resolves.toBe(false);

      const firstEvalCall = client.eval.mock.calls[0] as unknown[];

      expect(firstEvalCall[2]).toBe("emails:pending");
      expect(firstEvalCall[3]).toBe("emails:job:job-1");
      expect(firstEvalCall[4]).toBe("10");
      expect(firstEvalCall[5]).toBe("job-1");
      expect(job.createdAt).toBeDefined();
      expect(job.updatedAt).toBeDefined();
    });
  });

  describe("claim", () => {
    it("should claim a job via BRPOP when waitTimeoutMs > 0", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client, blockingClient } = getClients();

      const jobHash = createRedisJobHash();

      blockingClient.brpop.mockResolvedValueOnce(["emails:pending", "job-1"]);
      client.__multi.exec.mockResolvedValueOnce([
        [null, "OK"], // hset
        [null, 1], // hincrby
        [null, 1], // zadd
        [null, jobHash], // hgetall
      ]);

      const result = await adapter.claim("emails", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 5000,
      });

      expect(result).not.toBeNull();
      expect(result!.job.id).toBe("job-1");
      expect(result!.claimToken).toBeDefined();
      expect(blockingClient.brpop).toHaveBeenCalledWith("emails:pending", 5);
      expect(client.xadd).toHaveBeenCalledWith(
        "emails:capacity-events",
        "MAXLEN",
        "~",
        1000,
        "*",
        "type",
        "capacity_freed",
        "queue",
        "emails",
        "jobId",
        "job-1",
        "workerId",
        "worker-1",
        "at",
        expect.any(String)
      );
    });

    it("should return null when BRPOP times out", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { blockingClient } = getClients();

      blockingClient.brpop.mockResolvedValueOnce(null);

      const result = await adapter.claim("emails", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 5000,
      });

      expect(result).toBeNull();
    });

    it("should claim a job via Lua RPOP when waitTimeoutMs is 0", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      const jobHash = createRedisJobHash();
      client.eval.mockResolvedValueOnce(JSON.stringify(jobHash));

      const result = await adapter.claim("emails", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      expect(result).not.toBeNull();
      expect(result!.claimToken).toBeDefined();
      // Should use the claim Lua script, not BRPOP
      expect(client.eval).toHaveBeenCalledWith(
        expect.any(String), // claim.lua content
        2,
        "emails:pending",
        "emails:processing",
        "emails:job:",
        "worker-1",
        "30000",
        expect.any(String), // now
        expect.any(String) // claimToken UUID
      );
      expect(client.xadd).toHaveBeenCalledWith(
        "emails:capacity-events",
        "MAXLEN",
        "~",
        1000,
        "*",
        "type",
        "capacity_freed",
        "queue",
        "emails",
        "jobId",
        "job-1",
        "workerId",
        "worker-1",
        "at",
        expect.any(String)
      );
    });

    it("should return null when Lua claim returns null", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(null);

      const result = await adapter.claim("emails", {
        workerId: "worker-1",
        leaseMs: 30000,
        waitTimeoutMs: 0,
      });

      expect(result).toBeNull();
    });
  });

  describe("renewLease", () => {
    it("should renew lease via Lua script and return success", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(1);

      const result = await adapter.renewLease("emails", "job-1", "claim-token-123", 60000);

      expect(result).toBe(true);
      expect(client.eval).toHaveBeenCalledWith(
        expect.any(String), // renew-lease.lua content
        1,
        "emails:processing",
        "emails:job:job-1",
        "job-1",
        "claim-token-123",
        "60000",
        expect.any(String) // now
      );
    });

    it("should return false when claim token does not match", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(0);

      const result = await adapter.renewLease("emails", "job-1", "wrong-token", 60000);
      expect(result).toBe(false);
    });
  });

  describe("complete", () => {
    it("should complete a job via Lua script with claim token verification", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(1);

      const result = await adapter.complete("emails", "job-1", "claim-token-123");

      expect(result).toBe(true);
      expect(client.eval).toHaveBeenCalledWith(
        expect.any(String), // complete.lua content
        1,
        "emails:processing",
        "emails:job:job-1",
        "job-1",
        "claim-token-123",
        expect.any(String) // now
      );
      expect(client.xadd).toHaveBeenCalledWith(
        "emails:capacity-events",
        "MAXLEN",
        "~",
        1000,
        "*",
        "type",
        "job_terminal",
        "queue",
        "emails",
        "jobId",
        "job-1",
        "status",
        "completed",
        "at",
        expect.any(String)
      );
    });

    it("should return false when claim token is invalid", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(0);

      const result = await adapter.complete("emails", "job-1", "wrong-token");
      expect(result).toBe(false);
    });
  });

  describe("fail", () => {
    it("should fail a job with error message via Lua script", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(1);

      const result = await adapter.fail("emails", "job-1", "claim-token-123", "something broke");

      expect(result).toBe(true);
      expect(client.eval).toHaveBeenCalledWith(
        expect.any(String), // fail.lua content
        1,
        "emails:processing",
        "emails:job:job-1",
        "job-1",
        "claim-token-123",
        expect.any(String), // now
        "something broke"
      );
      expect(client.xadd).toHaveBeenCalledWith(
        "emails:capacity-events",
        "MAXLEN",
        "~",
        1000,
        "*",
        "type",
        "job_terminal",
        "queue",
        "emails",
        "jobId",
        "job-1",
        "status",
        "failed",
        "at",
        expect.any(String)
      );
    });

    it("should default error to empty string when not provided", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(1);

      await adapter.fail("emails", "job-1", "claim-token-123");

      const evalArgs = client.eval.mock.calls[0] as unknown[];
      expect(evalArgs[evalArgs.length - 1]).toBe("");
    });

    it("should return false when claim token is invalid", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(0);

      const result = await adapter.fail("emails", "job-1", "wrong-token", "error");
      expect(result).toBe(false);
    });
  });

  describe("retry", () => {
    it("should retry a job by moving to delayed set via Lua script", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();
      const executeAt = 1741392600000;

      client.eval.mockResolvedValueOnce(1);

      const result = await adapter.retry(
        "emails",
        "job-1",
        "claim-token-123",
        executeAt,
        "temp error"
      );

      expect(result).toBe(true);
      expect(client.eval).toHaveBeenCalledWith(
        expect.any(String), // retry.lua content
        2,
        "emails:processing",
        "emails:delayed",
        "emails:job:job-1",
        "job-1",
        "claim-token-123",
        expect.any(String), // now
        executeAt.toString(),
        "temp error"
      );
    });

    it("should default error to empty string when not provided", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(1);

      await adapter.retry("emails", "job-1", "claim-token-123", Date.now() + 5000);

      const evalArgs = client.eval.mock.calls[0] as unknown[];
      expect(evalArgs[evalArgs.length - 1]).toBe("");
    });

    it("should return false when claim token is invalid", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(0);

      const result = await adapter.retry("emails", "job-1", "wrong-token", Date.now() + 5000);
      expect(result).toBe(false);
    });
  });

  describe("promoteDelayedJobs", () => {
    it("should promote delayed jobs via Lua script", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.eval.mockResolvedValueOnce(3);

      await expect(adapter.promoteDelayedJobs("emails")).resolves.toBe(3);

      expect(client.eval).toHaveBeenCalledWith(
        expect.any(String),
        2,
        "emails:delayed",
        "emails:pending",
        "emails:job:",
        expect.any(String) // now
      );
    });
  });

  describe("recoverExpiredJobs", () => {
    it("should recover expired jobs via Lua script", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();
      const now = Date.now();

      client.eval.mockResolvedValueOnce(2);

      await expect(adapter.recoverExpiredJobs("emails", now)).resolves.toBe(2);

      expect(client.eval).toHaveBeenCalledWith(
        expect.any(String),
        2,
        "emails:processing",
        "emails:pending",
        "emails:job:",
        now.toString()
      );
    });
  });

  describe("query operations", () => {
    it("should peek and parse jobs stored in redis hashes", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.lindex.mockResolvedValueOnce(null).mockResolvedValueOnce("job-1");
      client.hgetall.mockResolvedValueOnce(
        createRedisJobHash({
          status: "processing",
          nextAttemptAt: "1741392000000",
          error: "boom",
        })
      );

      await expect(adapter.peek("emails")).resolves.toBeNull();

      const peekedJob = await adapter.peek("emails");
      expect(peekedJob).toEqual({
        id: "job-1",
        payload: { email: "job-1@test.com" },
        attempts: 1,
        maxAttempts: 5,
        status: "processing",
        nextAttemptAt: new Date(1741392000000),
        createdAt: 1741391000000,
        updatedAt: 1741391500000,
        processingStartedAt: 1741391600000,
        workerId: "worker-1",
        claimedAt: 1741391600000,
        leaseUntil: 1741421600000,
        claimToken: "test-token-123",
        error: "boom",
      });
    });

    it("should map empty optional redis fields to nullish runtime values", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.hgetall.mockResolvedValueOnce({
        id: "job-2",
        payload: JSON.stringify({ email: "empty@test.com" }),
        attempts: "0",
        maxAttempts: "1",
        status: "pending",
        nextAttemptAt: "",
        createdAt: "",
        updatedAt: "",
        processingStartedAt: "",
        workerId: "",
        claimedAt: "",
        leaseUntil: "",
        claimToken: "",
        error: "",
      });

      await expect(adapter.getJob("emails", "job-2")).resolves.toEqual({
        id: "job-2",
        payload: { email: "empty@test.com" },
        attempts: 0,
        maxAttempts: 1,
        status: "pending",
        nextAttemptAt: null,
        createdAt: undefined,
        updatedAt: undefined,
        processingStartedAt: undefined,
        workerId: undefined,
        claimedAt: undefined,
        leaseUntil: undefined,
        claimToken: undefined,
        error: null,
      });
    });

    it("should report queue size and fullness using redis list length", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.llen.mockResolvedValueOnce(4).mockResolvedValueOnce(10).mockResolvedValueOnce(0);

      await expect(adapter.size("emails")).resolves.toBe(4);
      await expect(adapter.isFull("emails")).resolves.toBe(true);
      await expect(adapter.isEmpty("emails")).resolves.toBe(true);
    });

    it("should return null for missing redis hashes", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.hgetall.mockResolvedValueOnce({});
      await expect(adapter.getJob("emails", "missing")).resolves.toBeNull();
    });

    it("should list processing job ids", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.zrange.mockResolvedValueOnce(["job-1", "job-2"]);

      await expect(adapter.getProcessingJobs("emails")).resolves.toEqual(["job-1", "job-2"]);
    });
  });

  describe("waitForCapacity", () => {
    it("should return true immediately when queue is not full", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.llen.mockResolvedValueOnce(4);

      await expect(adapter.waitForCapacity("emails", 5000)).resolves.toBe(true);
      expect(client.xrevrange).not.toHaveBeenCalled();
    });

    it("should wait on the stream when queue is full", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client, streamClient } = getClients();

      client.llen.mockResolvedValueOnce(10).mockResolvedValueOnce(10);
      client.xrevrange.mockResolvedValueOnce([["12-0", ["type", "capacity_freed"]]]);
      streamClient.xread.mockResolvedValueOnce([["emails:capacity-events", [["13-0", []]]]]);

      await expect(adapter.waitForCapacity("emails", 5000)).resolves.toBe(true);
      expect(client.xrevrange).toHaveBeenCalledWith("emails:capacity-events", "+", "-", "COUNT", 1);
      expect(streamClient.xread).toHaveBeenCalledWith(
        "BLOCK",
        5000,
        "STREAMS",
        "emails:capacity-events",
        "12-0"
      );
    });
  });

  describe("waitForTerminalState", () => {
    it("should return terminal job immediately when already completed", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client } = getClients();

      client.hgetall.mockResolvedValueOnce(createRedisJobHash({ status: "completed" }));

      await expect(adapter.waitForTerminalState("emails", "job-1", 5000)).resolves.toEqual(
        expect.objectContaining({
          id: "job-1",
          status: "completed",
        })
      );
      expect(client.xrevrange).not.toHaveBeenCalled();
    });

    it("should wait on the stream and return terminal job after wake", async () => {
      const adapter = new RedisStorageAdapter(config);
      const { client, streamClient } = getClients();

      client.hgetall
        .mockResolvedValueOnce(createRedisJobHash({ status: "processing" }))
        .mockResolvedValueOnce(createRedisJobHash({ status: "processing" }))
        .mockResolvedValueOnce(createRedisJobHash({ status: "failed", error: "boom" }));
      client.xrevrange.mockResolvedValueOnce([["20-0", ["type", "job_terminal"]]]);
      streamClient.xread.mockResolvedValueOnce([["emails:capacity-events", [["21-0", []]]]]);

      await expect(adapter.waitForTerminalState("emails", "job-1", 5000)).resolves.toEqual(
        expect.objectContaining({
          id: "job-1",
          status: "failed",
          error: "boom",
        })
      );
      expect(streamClient.xread).toHaveBeenCalledWith(
        "BLOCK",
        5000,
        "STREAMS",
        "emails:capacity-events",
        "20-0"
      );
    });
  });
});
