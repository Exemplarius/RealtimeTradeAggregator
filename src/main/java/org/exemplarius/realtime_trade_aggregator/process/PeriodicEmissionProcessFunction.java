package org.exemplarius.realtime_trade_aggregator.process;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.exemplarius.realtime_trade_aggregator.trade_transform.AggregatedTrade;
import org.exemplarius.realtime_trade_aggregator.utils.E9sLogger;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class PeriodicEmissionProcessFunction extends KeyedProcessFunction<Boolean, AggregatedTrade, AggregatedTrade>{
    private transient ValueState<AggregatedTrade> lastRecordState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<AggregatedTrade> descriptor = new ValueStateDescriptor<>(
                "lastAggregatedTrade", AggregatedTrade.class);
        lastRecordState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(AggregatedTrade record, Context ctx, Collector<AggregatedTrade> out) throws Exception {
        // Update the last record in state with the current windowed result
        lastRecordState.update(record);
        E9sLogger.logger.warn("HI");

        // Emit the record immediately
        out.collect(record);

        // Register a timer to emit every minute + 3 seconds
        long timerTimestamp = ctx.timerService().currentProcessingTime() + (60 * 1000) + 3000;
        ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregatedTrade> out) throws Exception {
        // Retrieve the last record from state
        AggregatedTrade lastRecord = lastRecordState.value();

        if (lastRecord != null) {
            // Modify the record as needed for synthetic emission
            AggregatedTrade modifiedRecord = modifyLastRecord(lastRecord, timestamp);

            // Emit the modified record
            out.collect(modifiedRecord);

            // Register the next timer for the following minute + 3 seconds
            ctx.timerService().registerProcessingTimeTimer(timestamp + (60 * 1000) + 3000);
        }
    }



    private AggregatedTrade modifyLastRecord(AggregatedTrade result, long timestamp) {
        Timestamp windowEnd = new Timestamp(timestamp);
        result.timestamp = windowEnd;

        // Convert to UTC while preserving the instant
        ZonedDateTime zdt = ZonedDateTime.of(windowEnd.toLocalDateTime(), ZoneId.systemDefault());
        ZonedDateTime utc = zdt.withZoneSameInstant(ZoneId.of("UTC"));

        result.timestamp_tf_rounded_ntz = Timestamp.valueOf(result.timestamp.toLocalDateTime()
                .atZone(ZoneId.of("UTC")).toLocalDateTime());
        result.timestamp_tf_rounded_tz = result.timestamp;
        result.processing_timestamp = Timestamp.valueOf(LocalDateTime.now());
        return result;
    }
}