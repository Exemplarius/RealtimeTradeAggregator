
# RealtimeTradeAggregator
A Flink application for realtime Bitmex trade stream transformations

This is a template application that originally reads Exemplarius kafka topics and aggregates
the data and insert it to postgres

The data output should not be delayed more than 3 seconds, the aggregation window is set to 1 minute, the
watermark is set to 3 seconds.

### JDBC Sink
https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/datastream/jdbc/


### Issue
```bash
2024-11-01 22:02:18 INFO  NetworkClient:937 - [AdminClient clientId=flink-trade-consumer-enumerator-admin-client] Node 1 disconnected.
```
Issue:
https://stackoverflow.com/questions/75526090/kafka-clients-3-2-3-node-disconnected-messages-frequently

Issue resolution:
https://docs.confluent.io/platform/current/installation/configuration/connect/index.html#connections-max-idle-ms


### Test topic structure
```java

    public static void test(Properties properties) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JsonNodeDeserializationSchema s = new JsonNodeDeserializationSchema();

        // TODO Update flink version and update to fix custom deserialzer to parse json
        FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<>(
                "alfa",
                s,
                properties
        );

        DataStream<String> alfa = env.addSource(kafkaConsumer)
                .map(a -> a.asText());
        alfa.print();

        env.execute("Flink Trade Aggregation");

    }


    public static void test2(Properties properties) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JsonNodeDeserializationSchema s = new JsonNodeDeserializationSchema();
        SimpleStringSchema sse = new SimpleStringSchema();
        // TODO Update flink version and update to fix custom deserialzer to parse json
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "alfa",
                sse,
                properties
        );

        DataStream<String> alfa = env.addSource(kafkaConsumer)
                .map(a -> a);
        alfa.print();

        env.execute("Flink Trade Aggregation");

    }
```


# Running
```
/usr/lib/jvm/java-11-openjdk/bin/java -cp target/RealtimeTradeAggregator-1.0-SNAPSHOT-jar-with-dependencies org.exemplarius.realtime_trade_aggregator.Main

```