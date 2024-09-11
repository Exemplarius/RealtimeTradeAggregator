package org.exemplarius.realtime_trade_aggregator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;
import org.exemplarius.realtime_trade_aggregator.trade_input.Trade;
import org.exemplarius.realtime_trade_aggregator.trade_input.TradeTable;
import org.exemplarius.realtime_trade_aggregator.trade_transform.AggregatedTrade;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeAggregateFunction;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeUnit;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeWindowFunction;
import org.exemplarius.realtime_trade_aggregator.utils.TimerBasedWatermarkGenerator;
import org.exemplarius.realtime_trade_aggregator.utils.TradeTableJsonDeserializationSchema;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.sql.Timestamp;
import java.util.stream.Collectors;

import static org.exemplarius.realtime_trade_aggregator.utils.TimestampUtils.greater;


public class Main {

    public static void tradeTransform2(Properties properties) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);

        JsonDeserializationSchema schema = new JsonDeserializationSchema<TradeTable>(TradeTable.class);
        JsonNodeDeserializationSchema s = new JsonNodeDeserializationSchema();
        TradeTableJsonDeserializationSchema tts = new TradeTableJsonDeserializationSchema();
        // TODO Update flink version and update to fix custom deserialzer to parse json
        FlinkKafkaConsumer<TradeTable> kafkaConsumer = new FlinkKafkaConsumer<>(
                "alfa",
                tts,
                properties
        );

//
        WatermarkStrategy<TradeTable> watermarkStrategy =
                WatermarkStrategy
                    .<TradeTable>forGenerator(ctx -> new TimerBasedWatermarkGenerator(Duration.ofSeconds(3L)))
                    .withTimestampAssigner((event, timestamp) -> {// event.get("timestamp").asLong()
                        List<Trade> trades = event.getData();
                        List<Timestamp> timestamps = trades.stream().map(Trade::getTimestamp).collect(Collectors.toList());
                        return timestamps.stream().reduce(timestamps.get(0), (trdA, trdB) -> {
                            Date a = greater(trdA, trdB);
                            if (trdA.compareTo(trdB) > 0) {return trdA; }
                            return trdB;
                        }).getTime();
                    });
                        //Watermark alignment might be better that timerbased .withWatermarkAlignment()
                        // idleness also
        DataStream<TradeUnit> tradeStream = env
                .addSource(kafkaConsumer)
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .flatMap(new FlatMapFunction<TradeTable, TradeUnit>() {
                    @Override
                    public void flatMap(TradeTable message, Collector<TradeUnit> collector) throws Exception {
                        message.getData().stream()
                            .forEach(m -> {
                                collector.collect(
                                    new TradeUnit(
                                        m.getTimestamp(),
                                        m.getSide(),
                                        m.getSize(),
                                        m.getPrice()
                                    )
                                );
                            });
                    }
                });


        DataStream<AggregatedTrade> aggregatedStream = tradeStream
                .keyBy(trade -> true) // Global window
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new TradeAggregateFunction(), new TradeWindowFunction());

        aggregatedStream.print();

        env.execute("Flink Trade Aggregation");
    }


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

    public static void main(String[] args) throws Exception {


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092");
        properties.setProperty("group.id", "flink-trade-consumer");

        //tradeTransform(properties);
        //test2(properties);
        tradeTransform2(properties);
    }


















}