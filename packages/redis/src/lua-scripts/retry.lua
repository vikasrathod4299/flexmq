-- Atomically retry a job (move to delayed set), verifying claim ownership
-- KEYS[1] = processingKey
-- KEYS[2] = delayedKey
-- ARGV[1] = jobKey
-- ARGV[2] = jobId
-- ARGV[3] = claimToken
-- ARGV[4] = now
-- ARGV[5] = executeAt (epoch ms when job should be retried)
-- ARGV[6] = error (optional, defaults to '')

local processingKey = KEYS[1]
local delayedKey = KEYS[2]
local jobKey = ARGV[1]
local jobId = ARGV[2]
local claimToken = ARGV[3]
local now = ARGV[4]
local executeAt = tonumber(ARGV[5])
local errorMsg = ARGV[6] or ''

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

-- Update job hash: mark delayed, clear claim metadata
redis.call('HSET', jobKey,
    'status', 'delayed',
    'error', errorMsg,
    'nextAttemptAt', executeAt,
    'updatedAt', now,
    'workerId', '',
    'claimedAt', '',
    'leaseUntil', '',
    'claimToken', '',
    'processingStartedAt', ''
)

-- Add to delayed sorted set with executeAt as score
redis.call('ZADD', delayedKey, executeAt, jobId)

return 1