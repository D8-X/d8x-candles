# d8x-candles
Second Evolution of Candle Stick Charts and index price service


## Install
- clone the repository
- install docker, you can run `./host-setup.sh` to do so
- copy .envExample to .env and change the value for REDIS_PW. Special characters in this password need to be escaped (e.g., $ needs to be written as \$)
- setup nginx/certbot to point to this machine
- run docker-compose `docker compose up --build -d`. Per default config, the websocket is available at port 8080/ws


## Websocket

- client subscribes (`"type": "subscribe"`) to a topic (`"topic": "<topic>"`) which is either 
  `"markets"` or `"<symbol>:<period>"` for candle-data, for example `"btc-usd:1m"`. Available
  periods are 1m, 5m, 15m, 1h, 1d (m: minute, h: hour, d: day)
- after each subscription the client receives a response `"type": "subscribe"` with the same topic, and a data array. The data array can contain
  an error `"data": ["error": "error message"]`, or it contains actual response data.
- after subscribing, the client will receive updates regularly `"type": "update"`
- the client has to unsubscribe from each topic `{"type": "unsubscribe", "topic":"btc-usd:15m"}`, or they will continue receiving data. 
- pings are handled on "protocol-level"

The client requests candle subscriptions via
```
{
  "type": "subscribe",
  "topic": "btc-usd:1m"
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


The client requests market summaries via
```
{
  "type": "subscribe",
  "topic": "markets"
}
```

to get a response like
```
{
  "topic": "markets",
  "data": [{
         "symbol":"chf-usdc",
         "assetType":"fx",
         "ret24hPerc":0.14039172448203116,
         "currentPx":1.121819392820981,
         "isOpen":true,
         "nextOpen":1694984400,
         "nextClose":1694984400
      },
      {
         "symbol":"usdc-usd",
         "assetType":"crypto",
         "ret24hPerc":-0.00540585710068383,
         "currentPx":1.00009996,
         "isOpen":true,
         "nextOpen":0,
         "nextClose":0
      }, ...]
}
```


## Developer Comments

<details>
<summary>
Supported periods are case-sensitive and as defined in the configuration file prices.config.json
</summary>

```
{ "period": "1m", "timeMs": 60000 },
{ "period": "5m", "timeMs": 350000 },
{ "period": "15m", "timeMs": 900000 },
{ "period": "1h", "timeMs": 3600000 },
{ "period": "1d", "timeMs": 86400000 }
```
</details>

## Testing 

```
docker run -d --name redis-stack -p 6379:6379 -e REDIS_ARGS="--requirepass yourpwd" redis/redis-stack-server:latest
```
- adjuste .env as indicated in .envExample comments
- PythClient
  `go run cmd/pyth-client/main.go`
- WsCandle
 `go run cmd/pyth-client/main.go`

### REDIS

**pub/sub:**

| **Pub/Sub Channel** | **Message Example** |
|---------------------|---------------------|
| px_update           | btc-usdc;btc-usd    |

Price observations are stored using the symbol of the format btc-usd as
key. Prices for triangulated series are also stored in this format.

**hset:**

```
hgetall btc-usd:mkt_info 
1) "is_open"
2) "true"
3) "nxt_open"
4) "null"
5) "nxt_close"
6) "null"
```
```
hgetall xau-usd:mkt_info
1) "is_open"
2) "true"
3) "nxt_open"
4) "1694379600"
5) "nxt_close"
6) "1694206800"
```
