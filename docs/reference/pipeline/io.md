# Inputs & outputs — Kafka messages and the Postgres sink

## Kafka input — `e9s.bitmex.trade.event`

JSON messages in the Bitmex websocket "table" shape, deserialized into `model/input/TradeTable`
by `TradeTableJsonDeserializationSchema` (corrupt messages → `null`, filtered out downstream):

```json
{
  "table": "trade",
  "action": "insert",
  "data": [
    {
      "timestamp": "…", "symbol": "XBTUSD", "side": "Buy",
      "size": 100, "price": 42000.5, "tickDirection": "PlusTick",
      "trdMatchID": "…", "grossValue": 238100, "homeNotional": 0.002381,
      "foreignNotional": 100, "trdType": "Regular"
    }
  ]
}
```

Only `timestamp`, `side`, `size`, `price` survive the flatMap into `TradeUnit`; the rest of the
`Trade` fields are parsed but unused by the aggregation.

## Kafka output — `e9s.bitmex.trade_aggregate.event`

`KafkaSink` with `AT_LEAST_ONCE` delivery; values are `AggregatedTrade` serialized by Jackson
(`AggregatedTradeSerializationSchema`), no key. The topic carries **both** intra-minute running
aggregates (one per input trade) and the finalized minute candle — consumers must be prepared for
repeated, monotonically-growing rows per minute followed by the final one (the final row is the
one with `timestamp`/`processing_timestamp` fields populated by the timer; running emissions have
null timestamps).

A third configured topic, `e9s.bitmex.trade_aggregate.batch`, is loaded into `KafkaTopic` but has
no getter/usage in the topology (reserved / dead config).

## Postgres output — `trade_volume_xbt_usd_min01`

`JdbcDatabaseSink.Elva(tableName)` builds a Flink `JdbcSink` (batch size 1000, batch interval
200 ms, 5 retries) against the shared `e9s_market_data` database; the schema comes from
`MARKET_SCHEMA` via `?currentSchema=` on the JDBC URL, so the insert uses the unqualified table
name. Only **finalized** minute rows reach the DB (one insert per minute).

Insert column order (all from `AggregatedTrade`):

| Columns | Content |
|---|---|
| `trades, buys, sells` | Trade counts (total / buy-side / sell-side). |
| `buy_volume, sell_volume` | Summed `size` per side. |
| `buy_open, buy_high, buy_low, buy_close` | Buy-side OHLC (`-1` = no buy trades yet). |
| `sell_open, sell_high, sell_low, sell_close` | Sell-side OHLC. |
| `last_buy_timestamp, last_sell_timestamp` | Time of the last trade per side (nullable). |
| `open, high, low, close` | Overall OHLC across both sides. |
| `timestamp` | Window end (minute boundary, UTC). |
| `timestamp_tf_rounded_ntz, timestamp_tf_rounded_tz` | Window end as no-timezone / with-timezone variants (identical while the JVM runs in UTC — see [concepts/configuration.md](../../concepts/configuration.md)). |
| `processing_timestamp` | Wall-clock time the row was finalized. |

The table itself (DDL) is owned by the database side of the ecosystem — this repo contains no
migration; it assumes the table exists in the `MARKET_SCHEMA` schema.

## Empty-minute rows

A minute with no trades still produces a row: counts and volumes are 0 and every price column is
flat at the previous candle's close (`TradeAggregationUtils.fromPreviousAggregatedTrade`). The
`last_*_timestamp` columns keep their previous values. Until the first trade is ever seen, no
rows are produced at all.
