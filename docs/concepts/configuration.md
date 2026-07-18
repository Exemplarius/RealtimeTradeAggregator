# Configuration — env vars, HOCON, and the underscore-host JDBC workaround

## Two configuration channels

| Channel | What it configures | Loaded by |
|---|---|---|
| Environment variables | PostgreSQL connection + schema | `config/JdbcConfigLoader` |
| HOCON `src/main/resources/application.conf` | Kafka bootstrap server/port + topic names | `config/KafkaConfigLoader` (Typesafe Config) |

### Environment variables (both required — startup fails loudly if missing)

| Var | Meaning | Example (`.env-example`) |
|---|---|---|
| `E9S_MARKET_DATABASE_URL` | Full Postgres connection string `postgresql://user:pass@host:port/db` | `postgresql://root:root@exemplarius.dev.machine:5432/e9s_market_data` |
| `MARKET_SCHEMA` | Postgres schema for the job's unqualified table names | `market` |

This is deliberate (commit `f980117`): the DB is declared as **one** connection-string var plus a
separate schema var, not split host/port/user/password fields, matching the post-2026-07
ecosystem convention where services share the consolidated `e9s_market_data` database and differ
only by schema. There is no HOCON `database` block anymore.

The schema is applied by appending `?currentSchema=<schema>` to the JDBC URL
(`JdbcDatabaseConfig.getJdbcUrl("postgres")`), so the SQL in the sink can keep unqualified table
names like `trade_volume_xbt_usd_min01`.

Workflow: copy `.env-example` → `.env`, edit, and let `build_and_run.sh` / `source-env.sh`
export it (`set -a; source .env; set +a`).

### HOCON (`application.conf`)

```
config.kafka.ingestion.server / .port        # bootstrap server (default: localhost:29092)
config.kafka.topic.trade_event               # input:  e9s.bitmex.trade.event
config.kafka.topic.trade_aggregate_event     # output: e9s.bitmex.trade_aggregate.event
config.kafka.topic.trade_aggregate_batch     # declared but currently unused
```

Note the Kafka server is baked into the packaged jar's `application.conf` (a commented-out
`exemplarius.dev.machine` line shows the deployed value). Changing it means editing the file and
rebuilding, or supplying a Typesafe-Config override.

## The underscore-host workaround (do not "simplify" this away)

`JdbcConfigLoader` parses `E9S_MARKET_DATABASE_URL` **manually** instead of using
`java.net.URI`'s accessors:

> `java.net.URI.getHost()` / `getUserInfo()` / `getPort()` silently return `null` / `-1` when the
> hostname contains an underscore — and docker-compose service names in the exemplarius
> infrastructure do (e.g. `exemplarius_postgres`, `candle_database`). An underscore is not a valid
> character in a registered DNS name, so the URI class refuses to treat the authority as
> host+port, without throwing.

The loader therefore takes `uri.getAuthority()` and splits `[user[:pass]@]host[:port]` itself
(port defaults to 5432, database = path minus the leading `/`). Any future change to config
loading must preserve this: round-trip a URL with an underscore host when touching
`JdbcConfigLoader`.

## Other fixed conventions

- `Main` forces the JVM default timezone to **UTC** before anything else; the timestamp fields
  written to the DB depend on this (see
  [reference/pipeline/io.md](../reference/pipeline/io.md)).
- Kafka consumer group is hard-coded to `flink-trade-consumer`, offsets start at `latest`,
  parallelism is 1, auto-watermark interval 1 s.
- The target table name `trade_volume_xbt_usd_min01` is hard-coded in `Main`
  (`JdbcDatabaseSink.Elva("trade_volume_xbt_usd_min01")`) — per-symbol/per-timeframe deployment
  currently means editing `Main`.
