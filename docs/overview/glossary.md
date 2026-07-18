# Glossary

One-liners for project terms; each points to the doc that covers it in depth.

- **TradeTable** — the raw Kafka input message: a Bitmex websocket `table` payload (`table`,
  `action`, `data: [Trade…]`). → [reference/pipeline/io.md](../reference/pipeline/io.md)
- **Trade** — one Bitmex trade inside a `TradeTable` (`timestamp`, `symbol`, `side`, `size`,
  `price`, …). → [reference/pipeline/io.md](../reference/pipeline/io.md)
- **TradeUnit** — the flattened per-trade record the pipeline actually processes
  (`timestamp`, `side`, `size`, `price`). → [reference/pipeline/processing.md](../reference/pipeline/processing.md)
- **TradeAccumulator** — running per-window state: counts, buy/sell volumes, buy/sell/total OHLC
  (prices initialized to `-1`). → [reference/pipeline/processing.md](../reference/pipeline/processing.md)
- **AggregatedTrade** — the finished 1-minute candle written to Postgres and published to Kafka.
  → [reference/pipeline/io.md](../reference/pipeline/io.md)
- **ContinuousTradeProcessor** — the active `KeyedProcessFunction`: minute-bucketed accumulation,
  processing-time timers (window end + 4 s), empty-window carry-forward.
  → [reference/pipeline/processing.md](../reference/pipeline/processing.md)
- **Empty-window carry-forward** — a minute with no trades still emits a candle: zero
  counts/volumes, all prices flat at the previous close.
  → [reference/pipeline/processing.md](../reference/pipeline/processing.md)
- **Kafka side output** — the processor's side stream (`kafka-output-stream` tag) carrying running
  and finalized aggregates to the `trade_aggregate` topic; the main output (DB) gets only
  finalized rows. → [reference/pipeline/processing.md](../reference/pipeline/processing.md)
- **TimerBasedWatermarkGenerator** — custom watermark generator: max seen event time minus 3 s,
  emitted at most every 3 s. → [reference/pipeline/processing.md](../reference/pipeline/processing.md)
- **Elva** — the factory method (`JdbcDatabaseSink.Elva(tableName)`) that builds the Flink JDBC
  sink for a given table. → [reference/pipeline/io.md](../reference/pipeline/io.md)
- **`trade_volume_xbt_usd_min01`** — the target Postgres table (hard-coded in `Main`) in the
  shared `e9s_market_data` database. → [reference/pipeline/io.md](../reference/pipeline/io.md)
- **`E9S_MARKET_DATABASE_URL` / `MARKET_SCHEMA`** — the two required env vars: one connection
  string + the schema for unqualified table names.
  → [concepts/configuration.md](../concepts/configuration.md)
- **Underscore-host workaround** — `java.net.URI` silently returns a null host when the hostname
  contains `_` (docker service names), so `JdbcConfigLoader` parses the URL authority manually.
  → [concepts/configuration.md](../concepts/configuration.md)
- **Legacy path** — window/trigger classes from earlier iterations
  (`TradeWindowFunction`, `EmptyAwareTradeWindowFunction`, `PeriodicEmissionProcessFunction`,
  `ProcessingTimeFallbackTrigger`) that are kept but not wired into the running topology.
  → [reference/pipeline/processing.md](../reference/pipeline/processing.md)
