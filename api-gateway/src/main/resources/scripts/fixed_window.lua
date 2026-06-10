-- fixed_window.lua
--
-- Atomic fixed-window-counter rate limiter (Phase 3).
--
-- The window key already encodes the current bucket - the caller computes
-- floor(now / window) and passes the bucketed key - so this script only has to
-- count within that bucket. Declaring the exact key in KEYS[1] (rather than
-- deriving it inside the script) keeps it correct under Redis Cluster.
--
-- KEYS[1] = bucketed rate-limit key (e.g. "rate_limit:fixed:127.0.0.1:29384756")
-- ARGV[1] = window size in milliseconds (used to set the bucket's TTL)
--
-- Returns the request count within the current bucket. The caller allows the
-- request while count <= limit.
--
-- INCR is atomic, so concurrent callers can never race the count - the property
-- fixed window keeps. What it gives up is fairness, not atomicity: a client can
-- spend a full quota just before a bucket boundary and a second full quota just
-- after it, up to 2x the limit across the boundary. The sliding-window log
-- (Phase 4) removes that boundary burst.

local key    = KEYS[1]
local window = tonumber(ARGV[1])

local count = redis.call('INCR', key)
if count == 1 then
    -- First request in this bucket: bound the bucket's lifetime to one window.
    redis.call('PEXPIRE', key, window)
end

return count
