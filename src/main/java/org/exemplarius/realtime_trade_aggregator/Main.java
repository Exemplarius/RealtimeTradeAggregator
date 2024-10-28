package org.exemplarius.realtime_trade_aggregator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.util.Collector;
import org.exemplarius.realtime_trade_aggregator.jdbc_sink.JdbcDatabaseSink;
import org.exemplarius.realtime_trade_aggregator.trade_input.Trade;
import org.exemplarius.realtime_trade_aggregator.trade_input.TradeTable;
import org.exemplarius.realtime_trade_aggregator.trade_transform.*;
import org.exemplarius.realtime_trade_aggregator.trigger.ProcessingTimeFallbackTrigger;
import org.exemplarius.realtime_trade_aggregator.utils.TimerBasedWatermarkGenerator;
import org.exemplarius.realtime_trade_aggregator.utils.TradeTableJsonDeserializationSchema;

import java.time.Duration;
import java.util.*;
import java.sql.Timestamp;
import java.util.stream.Collectors;

import static org.exemplarius.realtime_trade_aggregator.utils.TimestampUtils.greater;


public class Main {


    public static void main(String[] args) throws Exception {

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092");
        properties.setProperty("group.id", "flink-trade-consumer");

        //tradeTransform(properties);
        //test2(properties);
        tradeTransform2(properties);
    }

    public static void tradeTransform2(Properties properties) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);

        JsonDeserializationSchema schema = new JsonDeserializationSchema<TradeTable>(TradeTable.class);
        JsonNodeDeserializationSchema s = new JsonNodeDeserializationSchema();
        TradeTableJsonDeserializationSchema tts = new TradeTableJsonDeserializationSchema();


        FlinkKafkaConsumer<TradeTable> kafkaConsumer = new FlinkKafkaConsumer<>(
                "alfa",
                tts,
                properties
        );

        WatermarkStrategy<TradeTable> watermarkStrategy =
                WatermarkStrategy
                    .<TradeTable>forGenerator(ctx -> new TimerBasedWatermarkGenerator(Duration.ofSeconds(3L)))
                    //.withIdleness(Duration.ofMinutes(1))
                    .withTimestampAssigner((event, timestamp) -> {// event.get("timestamp").asLong()
                        List<Trade> trades = event.getData();
                        List<Timestamp> timestamps = trades.stream().map(Trade::getTimestamp).collect(Collectors.toList());
                        return timestamps.stream().reduce(timestamps.get(0), (trdA, trdB) -> {
                            Date a = greater(trdA, trdB);
                            if (trdA.compareTo(trdB) > 0) {return trdA; }
                            return trdB;
                        }).getTime();
                    });
                        //Watermark alignment might be better than timerbased .withWatermarkAlignment()
                        // for idleness also
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
                .trigger(new ProcessingTimeFallbackTrigger(Time.seconds(30)))
                // applies window function on the aggregated data
                .aggregate(new TradeAggregateFunction(), new EmptyAwareTradeWindowFunction());

        aggregatedStream.print();
        aggregatedStream.addSink(JdbcDatabaseSink.Elva("trade_volume_xbt_usd_min01"));
        env.execute("Flink Trade Aggregation");
    }






}