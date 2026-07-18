# Workflow — build & run

Supports local runs and the Raspbian (ARM) deployment. Supporting scripts:
`build_and_run.sh`, `source-env.sh` (repo root).

## 1. Prerequisites

- Maven + a JDK (bytecode targets Java 8; the runtime used in deployment is JDK 11).
- Kafka reachable at the server/port in `src/main/resources/application.conf`
  (default `localhost:29092`; deployed value `exemplarius.dev.machine` — if the config points at
  that host, map it in `/etc/hosts`, e.g. `192.66.777.32 exemplarius.dev.machine`).
- The `e9s_market_data` Postgres database reachable, with the target table
  `trade_volume_xbt_usd_min01` existing in your `MARKET_SCHEMA` schema (DDL is owned outside this
  repo).

## 2. Configure

```bash
cp .env-example .env    # then edit: E9S_MARKET_DATABASE_URL, MARKET_SCHEMA
```

Kafka server/topics are compiled into the jar from `application.conf` — edit and rebuild to
change them. See [../concepts/configuration.md](../concepts/configuration.md).

## 3. Build

```bash
mvn clean package
```

`package` runs both the assembly plugin (producing
`target/RealtimeTradeAggregator-1.0-SNAPSHOT-jar-with-dependencies.jar`) and the shade plugin;
`Main` is the manifest main class. (The README's older
`mvn clean package install assesmbly:single` line is a variant of the same thing with a typo —
plain `mvn clean package` suffices.)

## 4. Run

```bash
./build_and_run.sh
```

The script sources `.env` (auto-exporting all vars) and runs
`java -cp target/RealtimeTradeAggregator-1.0-SNAPSHOT-jar-with-dependencies.jar org.exemplarius.realtime_trade_aggregator.Main`
(note: the script hard-codes `/usr/lib/jvm/java-11-openjdk/bin/java` — adjust or use plain `java`
on machines with a different JDK layout). `source-env.sh` does only the env-loading half, for
running the class manually afterwards.

Despite the name, `build_and_run.sh` does **not** build — run `mvn clean package` first.

## 5. Verify

- Console shows the finalized candle print (`aggregatedStream.print()`) once per minute and
  `Processing window with N trades` log lines.
- One row per minute appears in `<MARKET_SCHEMA>.trade_volume_xbt_usd_min01`.
- Running aggregates flow on `e9s.bitmex.trade_aggregate.event`.

Known benign log noise: recurring `Node 1 disconnected` from the Kafka admin client — see the
"Issue" section of `README.md` (idle-connection reaping; tune `connections.max.idle.ms` if it
bothers you).

## Raspbian (ARM) deployment

From `README.md` — install Zulu JDK 11 for aarch64, then use the same build/run flow:

```sh
cd /opt
wget https://cdn.azul.com/zulu-embedded/bin/zulu11.35.36-ca-jdk11.0.5-linux_aarch64.tar.gz
tar -xvzf zulu*tar.gz
mv zulu11.35.36-ca-jdk11.0.5-linux_aarch64 jdk11
chmod -R 755 jdk11

# in ~/.bashrc:
export JAVA_HOME=/opt/jdk11
export PATH=$JAVA_HOME/bin:$PATH
```

Then `source ~/.bashrc` and use `build_and_run.sh` (adjust its hard-coded java path to
`/opt/jdk11/bin/java`).

TODO (automation candidate): make `build_and_run.sh` actually build when the jar is missing/stale
and take the java binary from `$JAVA_HOME` instead of a hard-coded path — trivial script change,
not done here to keep this a docs-only change.
