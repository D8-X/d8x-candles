package utils

// Params:
// KEYS[1] : sym:price_type
// ARGV[1] : fromTs
// ARGV[2] : toTs
// ARGV[3] : bucketDur
const LUA_OHLC = `
	local key = KEYS[1]
	local fromTs = ARGV[1]
	local toTs = ARGV[2]
	local bucketDur = ARGV[3]

	local results = {
		min = redis.call("TS.RANGE", key, fromTs, toTs, "ALIGN", "-", "AGGREGATION", "min", bucketDur),
		max = redis.call("TS.RANGE", key, fromTs, toTs, "ALIGN", "-", "AGGREGATION", "max", bucketDur),
		first = redis.call("TS.RANGE", key, fromTs, toTs, "ALIGN", "-", "AGGREGATION", "first", bucketDur),
		last = redis.call("TS.RANGE", key, fromTs, toTs, "ALIGN", "-", "AGGREGATION", "last", bucketDur),
	}
	return cjson.encode(results)
`
