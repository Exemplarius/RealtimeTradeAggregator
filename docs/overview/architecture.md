# Architecture — RealtimeTradeAggregator

## What this service is

RealtimeTradeAggregator is a single-job **Apache Flink 1.18.1** application (Java, compiled for
Java 8, run on JDK 11) that consumes the raw Bitmex trade stream from the exemplarius Kafka bus,
aggregates it into 1-minute buy/sell OHLC + volume candles, and delivers the result to two sinks:

1. **PostgreSQL** — one finalized row per minute into the `trade_volume_xbt_usd_min01` table of
   the shared `e9s_market_data` database (schema selected by the `MARKET_SCHEMA` env var).
2. **Kafka** — running (per-trade) and finalized aggregates published as JSON to the
   `e9s.bitmex.trade_aggregate.event` topic.

Target latency: output should lag the exchange by no more than ~3 seconds; the aggregation window
is 1 minute and the watermark out-of-orderness bound is 3 seconds (see `README.md`).

## Place in the exemplarius ecosystem

Upstream, an exemplarius ingestion service publishes Bitmex websocket `trade` table messages to
the `e9s.bitmex.trade.event` topic. This job is the aggregation step between that raw feed and
consumers of minute candles: downstream services read either the Postgres table (in the shared
`e9s_market_data` database, one of the two consolidated ecosystem databases) or the
`trade_aggregate` Kafka topic.

## Module layout

All code lives under `src/main/java/org/exemplarius/realtime_trade_aggregator/`:

| Package | Role |
|---|---|
| `Main.java` | Entry point — builds the whole Flink topology (`tradeTransform2`). |
| `config/` | Config loading: `KafkaConfigLoader` (HOCON `application.conf`) and `JdbcConfigLoader` (env vars, with a manual URL parser — see [concepts/configuration.md](../concepts/configuration.md)). |
| `model/input/` | Kafka input models — `TradeTable` (Bitmex table message) wrapping a list of `Trade`s. `KafkaTradeMessage` is unused. |
| `model/transform/` | `TradeUnit` (flattened trade), `TradeAccumulator` (running window state), `AggregatedTrade` (output row). |
| `model/serde/` | `TradeTableJsonDeserializationSchema` (input; returns `null` on corrupt JSON, filtered later) and `AggregatedTradeSerializationSchema` (Jackson JSON output). |
| `process/` | `ContinuousTradeProcessor` — the active windowing/emission logic. `PeriodicEmissionProcessFunction` and `TradeAggregationUtils` (accumulator → output conversion; empty-window carry-forward). |
| `trade_transform/` | `TradeAggregateFunction` (per-trade accumulation, used by the processor) plus legacy window functions. |
| `sink/` | `JdbcDatabaseSink.Elva(tableName)` — the Flink JDBC sink (batch 1000 rows / 200 ms, 5 retries). |
| `utils/` | `TimerBasedWatermarkGenerator`, `TimestampUtils`, `E9sLogger`. |
| `trigger/` | `ProcessingTimeFallbackTrigger` — legacy, not wired into the active topology. |

Several classes are **legacy alternatives** kept from earlier iterations and are not on the active
path: `TradeWindowFunction`, `EmptyAwareTradeWindowFunction`, `PeriodicEmissionProcessFunction`,
`ProcessingTimeFallbackTrigger`, `KafkaTradeMessage`, and the `FlinkKafkaConsumer` constructed but
never used in `Main`. See [reference/pipeline/processing.md](../reference/pipeline/processing.md).

## Data flow

```
Kafka e9s.bitmex.trade.event  (TradeTable JSON)
  │  KafkaSource + TimerBasedWatermarkGenerator (3 s out-of-orderness)
  ▼
filter nulls (corrupt messages deserialize to null)
  ▼
flatMap: TradeTable → one TradeUnit per contained Trade
  ▼
keyBy(true)  — single global key, parallelism 1
  ▼
ContinuousTradeProcessor (KeyedProcessFunction)
  ├─ per element: accumulate into the 1-min window bucket,
  │               emit running aggregate → Kafka side output
  └─ onTimer (window end + 4 s processing-time offset):
                 finalize the window (or carry forward the last
                 candle if the window had no trades)
        ├─ main output → JdbcDatabaseSink → Postgres trade_volume_xbt_usd_min01
        └─ side output → KafkaSink → e9s.bitmex.trade_aggregate.event
```

Details of windowing, watermarks, and the aggregation semantics:
[reference/pipeline/processing.md](../reference/pipeline/processing.md).
Message formats and the DB column list:
[reference/pipeline/io.md](../reference/pipeline/io.md).

## Build / run / test

Maven project (`pom.xml`); both the assembly plugin (`jar-with-dependencies`) and the shade plugin
run at `package`, each with `Main` as the manifest main class.

```bash
mvn clean package                    # builds target/RealtimeTradeAggregator-1.0-SNAPSHOT-jar-with-dependencies.jar
cp .env-example .env                 # then edit credentials
./build_and_run.sh                   # sources .env and runs Main on JDK 11
```

There is no test suite. Full playbook (including the Raspbian deployment recipe):
[workflows/build-and-run.md](../workflows/build-and-run.md).

## Configuration surface

- **Env vars** (required): `E9S_MARKET_DATABASE_URL` (single `postgresql://user:pass@host:port/db`
  connection string) and `MARKET_SCHEMA` (Postgres schema for the unqualified table names).
- **HOCON** `src/main/resources/application.conf`: Kafka bootstrap server/port and the three topic
  names (the third, `trade_aggregate_batch`, is loaded but currently unused).

See [concepts/configuration.md](../concepts/configuration.md) — including the deliberate manual
JDBC-URL parsing workaround for hostnames containing underscores.
