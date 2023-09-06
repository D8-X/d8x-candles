# d8x-candles
Second Evolution of Candle Stick Charts


https://github.com/RedisTimeSeries/redistimeseries-go

Aggregate:
RangeWithOptions

#Websocket

The client requests candle subscriptions via

Old:
```
{
  "type": "subscribe",
  "symbol": "btc-usd",
  "period": "1m"
}

```
New:
```
{
  "type": "subscribe",
  "topic": "btc-usd:1m",
}

```
Error:
```
{"type":"subscribe","topic":"xau-usvd:15m","data":{"error":"symbol not supported"}}
```
<details>
<summary>
Upon subscription the requestor receives an answer of the following form
</summary>


```
{
  "type": "subscribe",
  "msg": "btc-usdc:1m",
  "data": [{
    "start": 1678504920000,
    "time": "2023-03-11T03:22:00.000Z",
    "open": "20715.33957029727",
    "high": "20776.068733204742",
    "low": "20697.95967292916",
    "close": "20702.879084764278"
  }, {
    "start": 1678504980000,
    "time": "2023-03-11T03:23:00.000Z",
    "open": "20750.093849386834",
    "high": "20847.92663877994",
    "low": "20745.3458343564",
    "close": "20749.941669417913"
  }, ...]
}
```
</details>

<details>
<summary>
Updates are of the following form
</summary>
old:

```
{
  "type": "update",
  "topic": "btc-usdc:1m",
  "data": [{
    "start": 1678591260000,
    "time": "2023-03-12T03:21:00.000Z",
    "open": 20730.828392716117,
    "high": 20730.828392716117,
    "low": 20696.203770402524,
    "close": 20723.290900287615
  }]
}
```
new:
```
{{
  "type":"update",
  "topic":"btc-usd:1m",
  "data":
  {"start":1693831200000,
  "time":"2023-09-04T12:40:00.000Z",
  "open":25864.6240472,
  "high":25867.07622714,
  "low":25863.81874999,
  "close":25865.7231305}
}
```
</details>


Note that the array in data can contain more than one update element.
Only one period can be subscribed at a time for a given symbol. User should unsubscribe and re-subscribe. 

<details>
<summary>
Supported periods are case-sensitive and as defined in the configuration file live.config.json
</summary>

```
{ "period": "1m", "timeMs": 60000 },
{ "period": "5m", "timeMs": 350000 },
{ "period": "15m", "timeMs": 900000 },
{ "period": "1h", "timeMs": 3600000 },
{ "period": "1d", "timeMs": 86400000 }
```
</details>

The client requests market summaries via
```
{
  "type": "subscribe",
  "topic": "markets",
}
```

to get a response like
```
{
  "topic": "markets",
  "data": [{
    "symbol": "btc-usd",
    "24hpct": "20.02",
    "current": "30715.33957029727",
  }, {
    "symbol": "eth-usd",
    "24hpct": "00.01",
    "current": "1715.33957029727",
  }, ...]
}
```
A client request of the form `{"type": "ping"}` is responded with 
`{"type": "ping","msg": "pong"}`

## Testing 

```
docker run -d --name redis-stack -p 6379:6379 -e REDIS_ARGS="--requirepass yourpwd" redis/redis-stack-server:latest
```
## Developer Comments

###REDIS

**pub/sub:**

| **Pub/Sub Channel** | **Message Example** |
|---------------------|---------------------|
| px_update           | btc-usdc;btc-usd    |

Price observations are stored using the symbol of the format btc-usd as
key. Prices for triangulated series are also stored in this format.

**hset:**

```
hgetall btc-usd:mkt_hours 
1) "is_open"
2) "true"
3) "nxt_open"
4) "null"
5) "nxt_close"
6) "null"
```
```
hgetall xau-usd:mkt_hours
1) "is_open"
2) "true"
3) "nxt_open"
4) "1694379600"
5) "nxt_close"
6) "1694206800"
```