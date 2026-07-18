# Processing pipeline — windowing, watermarks, aggregation

The whole topology is built in `Main.tradeTransform2` (package
`org.exemplarius.realtime_trade_aggregator`). Parallelism is fixed at 1 and the stream is keyed
by the constant `true`, so there is exactly one logical window pipeline.

## Stages

1. **Source** — `KafkaSource<TradeTable>` on the `trade_event` topic, group
   `flink-trade-consumer`, starting offsets `latest`, value-only deserializer
   `TradeTableJsonDeserializationSchema` (returns `null` for corrupt JSON).
2. **Watermarks** — `TimerBasedWatermarkGenerator(3 s)` (in `utils/`): tracks the max event time
   seen and, on each periodic emit (auto-watermark interval 1 s), emits
   `max − 3 s` but only if that advances the last watermark by at least 3 s. The per-event
   timestamp assigner uses the **latest** trade timestamp inside the `TradeTable` batch.
3. **Null filter** — drops `null` elements (corrupt messages) with a log line.
4. **flatMap** — explodes each `TradeTable` into one `TradeUnit` per contained `Trade`
   (`timestamp`, `side`, `size`, `price`; `symbol` is dropped here — the job is single-market).
5. **`ContinuousTradeProcessor`** (in `process/`) — the heart of the job, a
   `KeyedProcessFunction<Boolean, TradeUnit, AggregatedTrade>`.

## ContinuousTradeProcessor semantics

- **Window bucketing.** Event time is floored to the minute (`WINDOW_SIZE = 60 000 ms`); each
  window's accumulator lives in `MapState<Long windowEnd, TradeAccumulator>`.
- **Accumulation.** Each `TradeUnit` is folded in via `TradeAggregateFunction.add`
  (in `trade_transform/`): per-side counts, volume, OHLC and last-trade timestamp, plus overall
  OHLC. Unset prices are `-1` (see `TradeAccumulator` defaults; commit `9ebe820`).
- **Running emission.** On *every* element the current accumulator is converted
  (`TradeAggregationUtils.fromAccumulator`) and emitted to the **Kafka side output** — downstream
  Kafka consumers see intra-minute running candles. The main (DB) output gets nothing here.
- **Finalization.** Each element registers a **processing-time** timer at
  `windowEnd + WINDOW_OFFSET` (offset 4 s, i.e. 1 s past the 3 s watermark bound). `onTimer`:
  - window had trades → finalize its accumulator;
  - window empty but a previous candle exists → `fromPreviousAggregatedTrade`: zero
    counts/volumes, all prices flat at the previous close (empty-window carry-forward);
  - no data ever seen → skip.
  The finalized row gets `timestamp` (= window end), `timestamp_tf_rounded_ntz` /
  `timestamp_tf_rounded_tz` and `processing_timestamp`, then goes to **both** the main output
  (→ JDBC sink) and the Kafka side output, is stored as `lastTradeState`, and the window state is
  cleared. The timer also re-arms itself one minute ahead, which keeps empty-window candles
  flowing when the feed goes quiet.
- Because the JVM timezone is forced to UTC in `Main`, the `_ntz` / `_tz` variants coincide in
  practice; the conversion logic in `onTimer` only matters if that changed.

## Quirks worth knowing (documented, not fixed)

- `fromAccumulator` maps `Double.MAX_VALUE` lows to `-1`, but since commit `9ebe820` the
  accumulator initializes lows to `-1` directly, so that guard is vestigial.
- `TradeAggregateFunction.merge` just returns the first accumulator — harmless at parallelism 1,
  wrong if the job were ever parallelized.
- Side classification is binary: `side == "Buy"` counts as buy, anything else as sell.
- `windowState()` returns a fresh accumulator when the key is missing; state is only persisted on
  `put`, which happens after every add.

## Legacy / inactive classes

Kept from earlier iterations of the same job (a windowed-aggregate design that was replaced by
`ContinuousTradeProcessor`); none are referenced by the active topology:

| Class | Was for |
|---|---|
| `trade_transform/TradeWindowFunction` | Plain tumbling-window finalization. |
| `trade_transform/EmptyAwareTradeWindowFunction` | Window function with last-value carry-forward via global state. |
| `process/PeriodicEmissionProcessFunction` | Periodic re-emission of the last aggregate. |
| `trigger/ProcessingTimeFallbackTrigger` | Tumbling-window trigger with a processing-time fallback for empty windows. |
| `model/input/KafkaTradeMessage` | Keyed Kafka envelope; the job reads value-only. |
| `FlinkKafkaConsumer` in `Main` | Old source API; constructed but not used (superseded by `KafkaSource`). Commented-out `test`/`test2` snippets live in `README.md`. |

If you change the active pipeline, don't assume these compile-time-only classes reflect current
behavior.
