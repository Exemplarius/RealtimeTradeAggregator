# CLAUDE.md

RealtimeTradeAggregator — a single-job Apache Flink 1.18.1 application (Java 8 bytecode, run on
JDK 11) that consumes the Bitmex trade stream from Kafka (`e9s.bitmex.trade.event`), aggregates
it into 1-minute buy/sell OHLC+volume candles, and writes them to Postgres
(`trade_volume_xbt_usd_min01` in the shared `e9s_market_data` DB) and back to Kafka
(`e9s.bitmex.trade_aggregate.event`).

## Build / run

```bash
mvn clean package        # → target/RealtimeTradeAggregator-1.0-SNAPSHOT-jar-with-dependencies.jar
cp .env-example .env     # edit E9S_MARKET_DATABASE_URL + MARKET_SCHEMA
./build_and_run.sh       # sources .env and runs Main (does NOT build; hard-codes a JDK 11 path)
```

There is no test suite. Kafka server/topics live in `src/main/resources/application.conf`
(compiled into the jar — edit + rebuild to change).

## Repo-specific constraints

- **Required env vars:** `E9S_MARKET_DATABASE_URL` (single `postgresql://user:pass@host:port/db`
  string) and `MARKET_SCHEMA`. Startup throws if either is missing.
- **Do not "simplify" `JdbcConfigLoader`** to use `java.net.URI.getHost()`: URI silently returns
  a null host for hostnames containing underscores (docker service names like
  `exemplarius_postgres`), which is why the URL authority is parsed manually. See
  `docs/concepts/configuration.md`.
- `Main` forces the JVM timezone to UTC; the DB timestamp columns depend on it.
- The target table name is hard-coded in `Main`; the table's DDL lives outside this repo.
- Several classes are legacy/inactive (old window-function path) — check
  `docs/reference/pipeline/processing.md` before assuming a class is on the live path.

## Documentation routing

The project's knowledge lives in `docs/`, but **do not read the whole `docs/` tree.** It is
indexed by a router so you load only what a task needs.

**Before loading any documentation:**

1. Read **`docs/ROUTES.md`** first — always. It is the single source of truth for documentation
   discovery.
2. Use its **Task → Documentation** table (or the **Index**) to find the one or two docs your
   task actually needs, and load **only** those.
3. For a single term, use `docs/overview/glossary.md` instead of loading a full doc. New to the
   service? Read `docs/overview/architecture.md` once for orientation.

**Keep the router in sync (do this in the same change, never later).** Whenever you add, remove,
rename, or substantially change a doc:

- [ ] Add/update/remove its row in the `docs/ROUTES.md` **Index**, and fix any affected
      **Task → Documentation** rows. **Every doc must be reachable from `ROUTES.md`** — an
      unrouted doc is an invisible doc.
- [ ] New term → add it to `docs/overview/glossary.md`.
- [ ] New repeatable process → add a `docs/workflows/` playbook and a `ROUTES.md` workflow row.
- [ ] Run `python3 docs/scripts/check_doc_links.py`; it must print **OK** (all links resolve,
      every doc is routed) before you commit.

Keep `docs/` itself updated as code changes so the context never drifts.

## Workflows and automation

Prefer reusable workflows and scripts over repeating manual work.

- If you find yourself doing a multi-step task that will recur (a deploy, an audit, a data
  backfill), capture it as a playbook in `docs/workflows/` and add it to `ROUTES.md`.
- If a workflow can be reduced by a script, write the script and reference it from the workflow;
  every script should document, in its header, the workflow it supports (see
  `docs/scripts/check_doc_links.py` for the pattern). Conversely, every workflow should name any
  supporting script under `ROUTES.md` → **Helper scripts & automation**.
