-- Atomically recover jobs with expired leases (lease-based recovery)
-- KEYS[1] = processingKey
-- KEYS[2] = pendingKey
-- ARGV[1] = jobKeyPrefix (e.g. "queueName:job:")
-- ARGV[2] = now (epoch ms)

local processingKey = KEYS[1]
local pendingKey = KEYS[2]
local jobKeyPrefix = ARGV[1]
local now = tonumber(ARGV[2])

-- Find all jobs whose lease has expired (leaseUntil <= now)
-- The processing sorted set uses leaseUntil as score
local expiredJobs = redis.call('ZRANGEBYSCORE', processingKey, 0, now)
local recovered = 0

for i, jobId in ipairs(expiredJobs) do
    local jobKey = jobKeyPrefix .. jobId

    -- Remove from processing set
    redis.call('ZREM', processingKey, jobId)

    -- Reset job to pending state, clear claim metadata
    redis.call('HSET', jobKey,
        'status', 'pending',
        'workerId', '',
        'claimedAt', '',
        'leaseUntil', '',
        'claimToken', '',
        'processingStartedAt', '',
        'updatedAt', now
    )

    -- Push back to pending queue
    redis.call('LPUSH', pendingKey, jobId)
    recovered = recovered + 1
end

return recovered
