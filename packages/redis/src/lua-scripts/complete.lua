-- Atomically complete a job, verifying claim ownership
-- KEYS[1] = processingKey
-- ARGV[1] = jobKey
-- ARGV[2] = jobId
-- ARGV[3] = claimToken
-- ARGV[4] = now

local processingKey = KEYS[1]
local jobKey = ARGV[1]
local jobId = ARGV[2]
local claimToken = ARGV[3]
local now = ARGV[4]

-- Verify claim token
local storedToken = redis.call('HGET', jobKey, 'claimToken')
if storedToken ~= claimToken then
    return 0
end

-- Verify job is in processing state
local status = redis.call('HGET', jobKey, 'status')
if status ~= 'processing' then
    return 0
end

-- Remove from processing set
redis.call('ZREM', processingKey, jobId)

-- Update job hash: mark completed, clear claim metadata
redis.call('HSET', jobKey,
    'status', 'completed',
    'updatedAt', now,
    'workerId', '',
    'claimedAt', '',
    'leaseUntil', '',
    'claimToken', '',
    'processingStartedAt', '',
    'nextAttemptAt', ''
)

-- Set TTL for auto-cleanup (24 hours)
redis.call('EXPIRE', jobKey, 86400)

return 1
