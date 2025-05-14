package utils

// Params:
// KEYS[1] : sym:price_type
// ARGV[1] : fromTs
// ARGV[2] : toTs
// ARGV[3] : bucketDur
// Without ALIGN, bucket start times are multiples of bucketDuration.
const LUA_OHLC = `
	local key = KEYS[1]
	local fromTs = ARGV[1]
	local toTs = ARGV[2]
	local bucketDur = ARGV[3]
	
	local results = {
		min = redis.call("TS.RANGE", key, fromTs, toTs, "ALIGN", 0, "AGGREGATION", "min", bucketDur),
		max = redis.call("TS.RANGE", key, fromTs, toTs,  "ALIGN", 0, "AGGREGATION", "max", bucketDur),
		first = redis.call("TS.RANGE", key, fromTs, toTs,  "ALIGN", 0, "AGGREGATION", "first", bucketDur),
		last = redis.call("TS.RANGE", key, fromTs, toTs,  "ALIGN", 0, "AGGREGATION", "last", bucketDur),
	}
	return cjson.encode(results)
`
