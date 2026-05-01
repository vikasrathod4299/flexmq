local delayedKey = KEYS[1]
local pendingKey = KEYS[2]
local jobKeyPrefix = ARGV[1]
local now = tonumber(ARGV[2])

local jobIds = redis.call('ZRANGEBYSCORE', delayedKey, 0, now)
local promoted = 0

for i, jobId in ipairs(jobIds) do
    redis.call('ZREM', delayedKey, jobId)
    redis.call('HSET', jobKeyPrefix .. jobId,
        'status', 'pending',
        'nextAttemptAt', '',
        'updatedAt', now
    )
    redis.call('LPUSH', pendingKey, jobId)
    promoted = promoted + 1
end
return promoted