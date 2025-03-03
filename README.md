# d8x-candles
Candle Stick Charts and index price service

## Prediction Markets
- Listens to polymarket websocket
- On price change queries the mid-price from stork (if credentials defined in .env), or else from polymarket API

## Regular Markets
- Streams Pyth prices (http)

## V2 and V3 Markets
- stream fixed indices based on config file

There could be clashes of symbols, for example ETH-USDC is likely sourced for V2 and Pyth. However, if the ticker is available
from a pyth-type source, we are not using this as an index from a v2 or v3 source. Therefore, we define a price source priority:
(1) Pyth, (2) V2, and (3) V3.


## Install
- clone the repository
- install docker, you can run `./host-setup.sh` to do so
- copy .envExample to .env and change the value for REDIS_PW. Special characters in this password need to be escaped (e.g., $ needs to be written as \$)
- specify stork credentials in .env, otherwise set to ""
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
- PolyClient
  `go run cmd/poly-client/main.go`
- WsCandle
 `go run cmd/ws-server/main.go`

### REDIS



**stream universe**


**prices**
On price updates, we add the price and all affected triangulations to Redis:
```
key := TYPE_PYTH+":"+sym //utils.PriceType +":"+sym
resp := (*client.Client).Do(client.Ctx,
		(*client.Client).B().TsAdd().Key(key).Timestamp(ts).Value(value).Build())
```

**pub/sub:**

| **Pub/Sub Channel** | **Message Example** |
|---------------------|---------------------|
| px_update           | BTC-USDC;BTC-USD    |
| ticker_request      | BTC-USDC            |

Price observations are stored using the symbol of the format btc-usd as
key. Prices for triangulated series are also stored in this format.

When the websocket-service receives a ticker request that it is not already available,
it will send a request `ticker_request` via pub channel and return "not available".

**Availability on Demand:**
If data is available, we set this in REDIS in the available ticker set 
`const AVAIL_TICKER_SET string = "avail"` concatenated with ":pricetype" (such as univ2): 
`SAdd(...)`. Index price feeds configured are always
made available. Triangulated tickers can be made available. Once we make a triangulated key available,
we also add the symbol to the available set.

On `ticker_request` triangulations are made available if possible.


**hset:**

```
hgetall BTC-USD:mkt_info 
1) "is_open"
2) "true"
3) "nxt_open"
4) "null"
5) "nxt_close"
6) "null"
```
```
hgetall XAU-USD:mkt_info
1) "is_open"
2) "true"
3) "nxt_open"
4) "1694379600"
5) "nxt_close"
6) "1694206800"
```

# Testing: redis
```
source .env
docker run -d --name redis-stack -p 6379:6379 -e REDIS_ARGS="--requirepass $REDIS_PW" redis/redis-stack-server:latest
docker exec -it redis-stack redis-cli -a $REDIS_PW
```
