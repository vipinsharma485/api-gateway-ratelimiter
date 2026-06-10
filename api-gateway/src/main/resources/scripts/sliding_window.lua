-- sliding_window.lua
--
-- Atomic sliding-window-log rate limiter, executed server-side by Redis.
--
-- WHY LUA: Redis runs a script as a single atomic unit - no other command can
-- interleave while it executes. That is exactly the guarantee the naive token
-- bucket lacked: a "GET count" followed by a "DECREMENT" spans two round trips,
-- so two concurrent requests on the same key can both read the same count and
-- both decide they are allowed (a check-then-act race). Here the read, the
-- decision and the write all happen atomically, so concurrent callers on the
-- same key can never both slip past the limit.
--
-- KEYS[1] = rate-limit key                 (e.g. "rate_limit:127.0.0.1")
-- ARGV[1] = current time in milliseconds
-- ARGV[2] = window size in milliseconds     (e.g. 60000 for one minute)
-- ARGV[3] = max requests allowed per window
-- ARGV[4] = unique member id for this request
--
-- Note on the member id: a sorted set member must be unique, but several
-- requests can share the same millisecond timestamp. The SCORE stays the
-- timestamp (that is what the window math needs); the MEMBER is a unique id
-- (timestamp + UUID) so same-millisecond requests are each counted instead of
-- collapsing into one entry.
--
-- Returns 1 if the request is allowed, 0 if it is rejected.

local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]

-- 1. Evict every entry that has fallen out of the trailing window.
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- 2. Count the requests still inside the window.
local count = redis.call('ZCARD', key)

if count < limit then
    -- 3a. Under the limit: record this request and allow it.
    redis.call('ZADD', key, now, member)
    -- Bound memory growth: let the key expire one full window after this write.
    redis.call('PEXPIRE', key, window)
    return 1
end

-- 3b. At or over the limit: reject without recording anything.
return 0
