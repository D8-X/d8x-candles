package globalrpc

// Params:
// KEYS[1] : key for curr index for the given chain
// KEYS[2] : key for urls for the given chain
// KEYS[3] : key for lock for the given chain
// ARGV[1] : expiry in seconds
const LUA_SCRIPT = `
	-- increment the round-robin index
	local index = redis.call("INCR", KEYS[1]) - 1
	local urls = redis.call("LRANGE", KEYS[2], 0, -1)
	local selectedUrl = urls[(index % #urls) + 1]
	local lockKey = KEYS[3] .. selectedUrl
	-- try to set the lock for this URL with a TTL
	local lockAcquired = redis.call("SET", lockKey, 1, "NX", "EX", ARGV[1])
	if lockAcquired then
		return selectedUrl
	else
		return nil
	end
`
