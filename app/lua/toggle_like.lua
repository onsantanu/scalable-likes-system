-- Runs atomically inside Redis: liked flag + queue append happen together or not at all.
-- The count is NOT touched here: the worker writes the exact count after each MySQL commit.
-- KEYS[1] = liked_key   KEYS[2] = count_key   KEYS[3] = stream_key
-- ARGV[1] = user_id  ARGV[2] = post_id  ARGV[3] = action ("like"/"unlike")
-- ARGV[4] = ts_ms    ARGV[5] = ttl seconds
if redis.call('EXISTS', KEYS[2]) == 0 then
    -- Count not cached: the API loads it once per post (also confirms the post exists)
    return 'NEED_LOAD'
end

local action = ARGV[3]
local state = redis.call('GET', KEYS[1])
-- Flag known and already in that state: nothing to do.
-- Flag unknown (never seen, or expired): queue the event anyway; the worker's
-- INSERT IGNORE / DELETE decides whether anything actually changes.
if action == 'like' and state == '1' then return 'NOOP' end
if action == 'unlike' and state == '0' then return 'NOOP' end

local ttl = tonumber(ARGV[5])
redis.call('SET', KEYS[1], action == 'like' and '1' or '0', 'EX', ttl)
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('XADD', KEYS[3], '*',
    'user_id', ARGV[1], 'post_id', ARGV[2], 'action', action, 'ts', ARGV[4])
return 'OK'
