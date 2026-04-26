-- Atomically renew a job's lease, verifying claim ownership
-- KEYS[1] = processingKey
-- ARGV[1] = jobKey
-- ARGV[2] = jobId
-- ARGV[3] = claimToken
-- ARGV[4] = leaseMs
-- ARGV[5] = now

local processingKey = KEYS[1]
local jobKey = ARGV[1]
local jobId = ARGV[2]
local claimToken = ARGV[3]
local leaseMs = tonumber(ARGV[4])
local now = tonumber(ARGV[5])

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

local leaseUntil = now + leaseMs

-- Update lease in job hash and processing sorted set score
redis.call('HSET', jobKey,
    'leaseUntil', leaseUntil,
    'updatedAt', now
)
redis.call('ZADD', processingKey, leaseUntil, jobId)

return 1
