# ROUTES — documentation router for RealtimeTradeAggregator

**This file is the single source of truth for documentation discovery.** Consult it *before*
loading any other doc. The repo is small — the docs are few, and most tasks need exactly one.

## How to use this file

1. Find your task in **[Task → Documentation](#task--documentation)** (or scan the **[Index](#index)**).
2. Load **only** the docs it names.
3. Need a term, not a whole doc? Use **[overview/glossary.md](overview/glossary.md)**.
4. New to the service? Read **[overview/architecture.md](overview/architecture.md)** once, then
   come back here.

Doc paths are relative to `docs/`; code paths are relative to the repo root.

---

## Vocabulary (quick)

Full list: **[overview/glossary.md](overview/glossary.md)**. The essentials:

- **TradeTable / Trade** — the raw Bitmex Kafka input message and the trades inside it.
- **TradeUnit → TradeAccumulator → AggregatedTrade** — flattened trade → running window state →
  finished 1-minute candle.
- **ContinuousTradeProcessor** — the active windowing logic (minute buckets, timer at +4 s,
  empty-window carry-forward).
- **Kafka side output** — running + finalized aggregates to `e9s.bitmex.trade_aggregate.event`;
  the DB gets only finalized rows.
- **`E9S_MARKET_DATABASE_URL` / `MARKET_SCHEMA`** — the two required env vars.
- **Underscore-host workaround** — manual JDBC-URL parsing in `JdbcConfigLoader` because
  `java.net.URI` returns a null host for underscore hostnames.

---

## Task → Documentation

| I want to… | Read (in order) |
|---|---|
| **Understand the whole service** | [overview/architecture.md](overview/architecture.md) → [reference/pipeline/processing.md](reference/pipeline/processing.md) |
| **Build / run / deploy (incl. Raspbian)** | [workflows/build-and-run.md](workflows/build-and-run.md) |
| **Change env vars / DB connection / Kafka config** | [concepts/configuration.md](concepts/configuration.md) |
| **Touch `JdbcConfigLoader` or the JDBC URL** | [concepts/configuration.md](concepts/configuration.md) (read the underscore-host section first) |
| **Change windowing / watermark / aggregation logic** | [reference/pipeline/processing.md](reference/pipeline/processing.md) |
| **Change input/output message shape or the DB row** | [reference/pipeline/io.md](reference/pipeline/io.md) → [reference/pipeline/processing.md](reference/pipeline/processing.md) |
| **Consume this service's output (Kafka topic or table)** | [reference/pipeline/io.md](reference/pipeline/io.md) |
| **Understand a term** | [overview/glossary.md](overview/glossary.md) |

---

## Code map — where the code lives

Package root: `src/main/java/org/exemplarius/realtime_trade_aggregator/`

| Subsystem | Source |
|---|---|
| Topology / entry point | `Main.java` (`tradeTransform2`) |
| Config loading | `config/` — `KafkaConfigLoader` (+ `src/main/resources/application.conf`), `JdbcConfigLoader` (env vars) |
| Input models & serde | `model/input/`, `model/serde/` |
| Aggregation state & output model | `model/transform/` |
| Active windowing logic | `process/ContinuousTradeProcessor.java`, `process/TradeAggregationUtils.java`, `trade_transform/TradeAggregateFunction.java` |
| Postgres sink | `sink/JdbcDatabaseSink.java` |
| Watermarks / logging / time utils | `utils/` |
| Legacy (inactive) window path | `trade_transform/TradeWindowFunction.java`, `trade_transform/EmptyAwareTradeWindowFunction.java`, `process/PeriodicEmissionProcessFunction.java`, `trigger/ProcessingTimeFallbackTrigger.java` |
| Build / launch | `pom.xml`, `build_and_run.sh`, `source-env.sh`, `.env-example` |

---

## Index

### `overview/` — orientation
| Doc | When to consult |
|---|---|
| [overview/architecture.md](overview/architecture.md) | What the service is, ecosystem fit, module layout, data-flow diagram, build/run summary. Load first if new. |
| [overview/glossary.md](overview/glossary.md) | One-line definition of any project term + pointer to its deep doc. |

### `concepts/` — cross-cutting *why*
| Doc | When to consult |
|---|---|
| [concepts/configuration.md](concepts/configuration.md) | Env-var scheme (`E9S_MARKET_DATABASE_URL` + `MARKET_SCHEMA`), HOCON Kafka config, the deliberate underscore-host JDBC parsing workaround, UTC/timezone and other fixed conventions. Read before touching config code. |

### `reference/pipeline/` — the Flink job
| Doc | When to consult |
|---|---|
| [reference/pipeline/processing.md](reference/pipeline/processing.md) | Topology stages, watermark strategy, `ContinuousTradeProcessor` semantics, quirks, legacy/inactive classes. |
| [reference/pipeline/io.md](reference/pipeline/io.md) | Kafka input/output message formats, topic semantics (running vs finalized aggregates), the Postgres insert column list, empty-minute rows. |

### `workflows/` — repeatable playbooks
| Doc | When to consult | Supporting scripts |
|---|---|---|
| [workflows/build-and-run.md](workflows/build-and-run.md) | Building the fat jar, configuring `.env`, running locally, verifying output, Raspbian deployment. | `build_and_run.sh`, `source-env.sh` |

---

## Helper scripts & automation

| Script | What it does | Supports |
|---|---|---|
| [scripts/check_doc_links.py](scripts/check_doc_links.py) | Verifies every relative doc link resolves **and** every doc is referenced in this file. Run after any doc change. | The [Maintenance](#maintenance) workflow below |
| `build_and_run.sh` (repo root) | Sources `.env` and runs the packaged jar (does **not** build). | [workflows/build-and-run.md](workflows/build-and-run.md) |
| `source-env.sh` (repo root) | Exports `.env` vars into the current shell for manual runs. | [workflows/build-and-run.md](workflows/build-and-run.md) |

---

## Maintenance

Keep this router in lockstep with the docs. **Whenever you add, remove, rename, or substantially
change a doc:**

- [ ] Put the doc in the right folder: `reference/pipeline/` for deep job docs, `concepts/` for
      cross-cutting *why*/conventions, `workflows/` for a repeatable process, `overview/` for
      orientation.
- [ ] Update the doc's row in the **[Index](#index)**.
- [ ] Update any affected **[Task → Documentation](#task--documentation)** rows.
- [ ] New term → add it to [overview/glossary.md](overview/glossary.md).
- [ ] New repeatable process → add a `workflows/` doc and a row here (plus its supporting script
      under **[Helper scripts](#helper-scripts--automation)**).
- [ ] Run `python3 docs/scripts/check_doc_links.py` — it must report **OK** before you commit.
