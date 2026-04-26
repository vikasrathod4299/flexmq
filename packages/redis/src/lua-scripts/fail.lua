-- Atomically fail a job, verifying claim ownership
-- KEYS[1] = processingKey
-- ARGV[1] = jobKey
-- ARGV[2] = jobId
-- ARGV[3] = claimToken
-- ARGV[4] = now
-- ARGV[5] = error (optional, defaults to '')

local processingKey = KEYS[1]
local jobKey = ARGV[1]
local jobId = ARGV[2]
local claimToken = ARGV[3]
local now = ARGV[4]
local errorMsg = ARGV[5] or ''

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

-- Update job hash: mark failed, clear claim metadata
redis.call('HSET', jobKey,
    'status', 'failed',
    'error', errorMsg,
    'updatedAt', now,
    'workerId', '',
    'claimedAt', '',
    'leaseUntil', '',
    'claimToken', '',
    'processingStartedAt', '',
    'nextAttemptAt', ''
)

return 1
