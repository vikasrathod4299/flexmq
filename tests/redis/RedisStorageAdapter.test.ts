const mockRedisInstances: MockRedisClient[] = [];

type MockRedisMulti = {
  hset: jest.Mock;
  zadd: jest.Mock;
  zrem: jest.Mock;
  expire: jest.Mock;
  exec: jest.Mock;
};

type MockRedisClient = {
  options: unknown;
  ping: jest.Mock;
  quit: jest.Mock;
  eval: jest.Mock;
  brpop: jest.Mock;
  lindex: jest.Mock;
  hgetall: jest.Mock;
  llen: jest.Mock;
  multi: jest.Mock;
  zadd: jest.Mock;
  hset: jest.Mock;
  zrange: jest.Mock;
  __multi: MockRedisMulti;
};

const mockCreateRedisClient = (options: unknown): MockRedisClient => {
  const multi: MockRedisMulti = {
    hset: jest.fn().mockReturnThis(),
    zadd: jest.fn().mockReturnThis(),
    zrem: jest.fn().mockReturnThis(),
    expire: jest.fn().mockReturnThis(),
    exec: jest.fn().mockResolvedValue([]),
  };

  const client: MockRedisClient = {
    options,
    ping: jest.fn().mockResolvedValue('PONG'),
    quit: jest.fn().mockResolvedValue('OK'),
    eval: jest.fn(),
    brpop: jest.fn(),
    lindex: jest.fn(),
    hgetall: jest.fn(),
    llen: jest.fn(),
    multi: jest.fn(() => multi),
    zadd: jest.fn().mockResolvedValue(1),
    hset: jest.fn().mockResolvedValue(1),
    zrange: jest.fn().mockResolvedValue([]),
    __multi: multi,
  };

  mockRedisInstances.push(client);
  return client;
};

jest.mock('ioredis', () => ({
  __esModule: true,
  default: jest.fn().mockImplementation((options) => mockCreateRedisClient(options)),
}));

import Redis from 'ioredis';
import { type Job } from 'flexmq';
import { RedisStorageAdapter } from '@flexmq/redis';

const createJob = (id: string): Job<{ email: string }> => ({
  id,
  payload: { email: `${id}@test.com` },
  attempts: 1,
  maxAttempts: 5,
  status: 'pending',
  nextAttemptAt: null,
  error: null,
});

describe('RedisStorageAdapter', () => {
  const config = {
    host: 'localhost',
    port: 6379,
    password: 'secret',
    capacity: 10,
  };

  const getClients = () => {
    const [client, blockingClient] = mockRedisInstances;
    return { client, blockingClient };
  };

  beforeEach(() => {
    mockRedisInstances.length = 0;
    jest.clearAllMocks();
  });

  it('should create two redis clients with the expected options', () => {
    new RedisStorageAdapter(config);

    const RedisMock = Redis as unknown as jest.Mock;
    expect(RedisMock).toHaveBeenCalledTimes(2);
    expect(RedisMock).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        host: 'localhost',
        port: 6379,
        password: 'secret',
        maxRetriesPerRequest: null,
      })
    );
  });

  it('should connect and disconnect both redis clients', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client, blockingClient } = getClients();

    await adapter.connect();
    await adapter.disconnect();

    expect(client.ping).toHaveBeenCalledTimes(1);
    expect(blockingClient.ping).toHaveBeenCalledTimes(1);
    expect(client.quit).toHaveBeenCalledTimes(1);
    expect(blockingClient.quit).toHaveBeenCalledTimes(1);
  });

  it('should enqueue jobs through the Lua script and return true only for successful writes', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();
    const job = createJob('job-1');

    client.eval.mockResolvedValueOnce(1).mockResolvedValueOnce(0);

    await expect(adapter.enqueue('emails', job)).resolves.toBe(true);
    await expect(adapter.enqueue('emails', createJob('job-2'))).resolves.toBe(false);

    const firstEvalCall = client.eval.mock.calls[0];
    const serializedJob = JSON.parse(firstEvalCall[6]);

    expect(firstEvalCall).toEqual([
      expect.any(String),
      2,
      'emails:pending',
      'emails:job:job-1',
      '10',
      'job-1',
      expect.any(String),
    ]);
    expect(serializedJob.payload).toEqual(job.payload);
    expect(job.createdAt).toBeDefined();
    expect(job.updatedAt).toBeDefined();
  });

  it('should dequeue jobs through BRPOP and return null when no job is acquired', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client, blockingClient } = getClients();
    const acquiredJob = createJob('job-1');
    acquiredJob.status = 'processing';

    blockingClient.brpop
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce(['emails:pending', 'job-1'])
      .mockResolvedValueOnce(['emails:pending', 'job-2']);
    client.eval.mockResolvedValueOnce(JSON.stringify(acquiredJob)).mockResolvedValueOnce(null);

    await expect(adapter.dequeue('emails')).resolves.toBeNull();
    await expect(adapter.dequeue('emails', 2)).resolves.toEqual(acquiredJob);
    await expect(adapter.dequeue('emails', 2)).resolves.toBeNull();

    expect(client.eval).toHaveBeenCalledWith(
      expect.any(String),
      2,
      'emails:processing',
      'emails:job:job-1',
      expect.any(String),
      'job-1'
    );
  });

  it('should peek and parse jobs stored in redis hashes', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();

    client.lindex.mockResolvedValueOnce(null).mockResolvedValueOnce('job-1');
    client.hgetall.mockResolvedValueOnce({
      id: 'job-1',
      payload: JSON.stringify({ email: 'peek@test.com' }),
      attempts: '2',
      maxAttempts: '5',
      status: 'processing',
      nextAttemptAt: '1741392000000',
      createdAt: '1741391000000',
      updatedAt: '1741391500000',
      processingStartedAt: '1741391600000',
      workerId: 'worker-1',
      error: 'boom',
    });

    await expect(adapter.peek('emails')).resolves.toBeNull();

    const peekedJob = await adapter.peek('emails');
    expect(peekedJob).toEqual({
      id: 'job-1',
      payload: { email: 'peek@test.com' },
      attempts: 2,
      maxAttempts: 5,
      status: 'processing',
      nextAttemptAt: new Date(1741392000000),
      createdAt: 1741391000000,
      updatedAt: 1741391500000,
      processingStartedAt: 1741391600000,
      workerId: 'worker-1',
      error: 'boom',
    });
  });

  it('should map empty optional redis fields to nullish runtime values', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();

    client.hgetall.mockResolvedValueOnce({
      id: 'job-2',
      payload: JSON.stringify({ email: 'empty@test.com' }),
      attempts: '0',
      maxAttempts: '1',
      status: 'pending',
      nextAttemptAt: '',
      createdAt: '',
      updatedAt: '',
      processingStartedAt: '',
      workerId: '',
      error: '',
    });

    await expect(adapter.getJob('emails', 'job-2')).resolves.toEqual({
      id: 'job-2',
      payload: { email: 'empty@test.com' },
      attempts: 0,
      maxAttempts: 1,
      status: 'pending',
      nextAttemptAt: null,
      createdAt: undefined,
      updatedAt: undefined,
      processingStartedAt: undefined,
      workerId: undefined,
      error: null,
    });
  });

  it('should report queue size and fullness using redis list length', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();

    client.llen.mockResolvedValueOnce(4).mockResolvedValueOnce(10).mockResolvedValueOnce(0);

    await expect(adapter.size('emails')).resolves.toBe(4);
    await expect(adapter.isFull('emails')).resolves.toBe(true);
    await expect(adapter.isEmpty('emails')).resolves.toBe(true);
  });

  it('should schedule delayed jobs via a redis transaction', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();
    const job = createJob('job-1');
    const executeAt = 1741392600000;

    await adapter.scheduleDelayed('emails', job, executeAt);

    expect(client.multi).toHaveBeenCalledTimes(1);
    expect(client.__multi.hset).toHaveBeenCalledWith(
      'emails:job:job-1',
      'updatedAt',
      expect.any(String),
      'nextAttemptAt',
      executeAt.toString()
    );
    expect(client.__multi.zadd).toHaveBeenCalledWith('emails:delayed', executeAt, 'job-1');
    expect(client.__multi.exec).toHaveBeenCalledTimes(1);
    expect(job.nextAttemptAt?.getTime()).toBe(executeAt);
  });

  it('should mark jobs as processing, completed, and failed', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();

    await adapter.markProcessing('emails', 'job-1', 'worker-1');
    expect(client.zadd).toHaveBeenCalledWith('emails:processing', expect.any(Number), 'job-1');
    expect(client.hset).toHaveBeenCalledWith(
      'emails:job:job-1',
      'status',
      'processing',
      'processingStartedAt',
      expect.any(String),
      'workerId',
      'worker-1',
      'updatedAt',
      expect.any(String)
    );

    await adapter.markCompleted('emails', 'job-1');
    expect(client.__multi.zrem).toHaveBeenCalledWith('emails:processing', 'job-1');
    expect(client.__multi.hset).toHaveBeenCalledWith(
      'emails:job:job-1',
      'status',
      'completed',
      'processingStartedAt',
      '',
      'updatedAt',
      expect.any(String)
    );
    expect(client.__multi.expire).toHaveBeenCalledWith('emails:job:job-1', 86400);

    client.__multi.zrem.mockClear();
    client.__multi.hset.mockClear();
    client.__multi.exec.mockClear();

    await adapter.markFailed('emails', 'job-2', 'failed hard');
    expect(client.__multi.zrem).toHaveBeenCalledWith('emails:processing', 'job-2');
    expect(client.__multi.hset).toHaveBeenCalledWith(
      'emails:job:job-2',
      'status',
      'failed',
      'processingStartedAt',
      '',
      'error',
      'failed hard',
      'updatedAt',
      expect.any(String)
    );
  });

  it('should default failed job errors to an empty string', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();

    await adapter.markFailed('emails', 'job-3');

    expect(client.__multi.hset).toHaveBeenCalledWith(
      'emails:job:job-3',
      'status',
      'failed',
      'processingStartedAt',
      '',
      'error',
      '',
      'updatedAt',
      expect.any(String)
    );
  });

  it('should promote delayed jobs, recover stuck jobs, and list processing jobs', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();

    client.eval.mockResolvedValueOnce(3).mockResolvedValueOnce(2);
    client.zrange.mockResolvedValueOnce(['job-1', 'job-2']);

    await expect(adapter.promoteDelayedJobs('emails')).resolves.toBe(3);
    await expect(adapter.recoverStuckJobs('emails', 30000)).resolves.toBe(2);
    await expect(adapter.getProcessingJobs('emails')).resolves.toEqual(['job-1', 'job-2']);

    expect(client.eval).toHaveBeenNthCalledWith(
      1,
      expect.any(String),
      2,
      'emails:delayed',
      'emails:pending',
      'emails:job:',
      expect.any(Number)
    );
    expect(client.eval).toHaveBeenNthCalledWith(
      2,
      expect.any(String),
      2,
      'emails:processing',
      'emails:pending',
      'emails:job:',
      expect.any(Number),
      30000
    );
  });

  it('should update jobs and return null for missing redis hashes', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();
    const job = createJob('job-3');
    job.status = 'failed';
    job.nextAttemptAt = new Date(1741392700000);

    await adapter.updateJob('emails', job);
    expect(client.hset).toHaveBeenCalledWith(
      'emails:job:job-3',
      'payload',
      JSON.stringify(job.payload),
      'attempts',
      '1',
      'maxAttempts',
      '5',
      'status',
      'failed',
      'nextAttemptAt',
      '1741392700000',
      'updatedAt',
      expect.any(String),
      'error',
      ''
    );

    client.hgetall.mockResolvedValueOnce({});
    await expect(adapter.getJob('emails', 'missing')).resolves.toBeNull();
  });

  it('should serialize empty retry timestamps and explicit errors when updating jobs', async () => {
    const adapter = new RedisStorageAdapter(config);
    const { client } = getClients();
    const job = createJob('job-4');
    job.status = 'completed';
    job.nextAttemptAt = null;
    job.error = 'done';

    await adapter.updateJob('emails', job);

    expect(client.hset).toHaveBeenCalledWith(
      'emails:job:job-4',
      'payload',
      JSON.stringify(job.payload),
      'attempts',
      '1',
      'maxAttempts',
      '5',
      'status',
      'completed',
      'nextAttemptAt',
      '',
      'updatedAt',
      expect.any(String),
      'error',
      'done'
    );
  });
});