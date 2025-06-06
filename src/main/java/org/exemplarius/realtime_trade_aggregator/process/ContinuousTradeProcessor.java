package org.exemplarius.realtime_trade_aggregator.process;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.exemplarius.realtime_trade_aggregator.model.transform.AggregatedTrade;
import org.exemplarius.realtime_trade_aggregator.model.transform.TradeAccumulator;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeAggregateFunction;
import org.exemplarius.realtime_trade_aggregator.model.transform.TradeUnit;
import org.exemplarius.realtime_trade_aggregator.utils.E9sLogger;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ContinuousTradeProcessor extends KeyedProcessFunction<Boolean, TradeUnit, AggregatedTrade> {
    private static final long WINDOW_SIZE = 60 * 1000; // 1 minute in milliseconds
    private static final long WINDOW_OFFSET = 4000;
    private ValueState<AggregatedTrade> lastTradeState;
    private MapState<Long, TradeAccumulator> activeWindowStates;

    public static final OutputTag<AggregatedTrade> kafkaSideOutput =
            new OutputTag<AggregatedTrade>("kafka-output-stream") {};

    private TradeAccumulator windowState(Long key) throws Exception {
        if (activeWindowStates.contains(key)) {
            return activeWindowStates.get(key);
        }
        return new TradeAccumulator();
    }

    @Override
    public void processElement(TradeUnit trade, Context ctx, Collector<AggregatedTrade> out) throws Exception {


        // Align to minute boundaries
        long eventTime = trade.timestamp.getTime();
        long currentWindowStart = (eventTime / WINDOW_SIZE) * WINDOW_SIZE;

        // Register timer for this window if not already registered
        long windowEnd = currentWindowStart + WINDOW_SIZE;


        // most likely we will need to adjust this to emit datat more often
        ctx.timerService().registerProcessingTimeTimer(windowEnd + WINDOW_OFFSET);

        // register second based timer every second
        //ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + 1000 );


        // Get or create accumulator for current window
        TradeAccumulator acc = windowState(windowEnd);
        // Use existing aggregate function logic
        TradeAggregateFunction aggregator = new TradeAggregateFunction();

        acc = aggregator.add(trade, acc);

        activeWindowStates.put(windowEnd, acc);

        ctx.output(kafkaSideOutput, TradeAggregationUtils.fromAccumulator(acc));
        //E9sLogger.logger.info("Processed trade for window ending at: " + new Timestamp(windowEnd));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregatedTrade> out) throws Exception {

        Long windowEndTimestamp = timestamp - WINDOW_OFFSET;

        AggregatedTrade lastTrade = lastTradeState.value();

        AggregatedTrade result = new AggregatedTrade();
        if (activeWindowStates.contains(windowEndTimestamp)) {
            // We had trades in this window - use accumulated data
            TradeAccumulator acc = activeWindowStates.get(windowEndTimestamp);

            result = TradeAggregationUtils.fromAccumulator(acc);
            E9sLogger.logger.info("Processing window with " + acc.trades + " trades");
        } else if (lastTrade != null) {
            // Empty window - use last trade values
            result = TradeAggregationUtils.fromPreviousAggregatedTrade(lastTrade);

            E9sLogger.logger.info("Processing empty window, using last known values");
        } else {
            E9sLogger.logger.info("No data available yet, skipping window");
            return;
        }
        E9sLogger.logger.warn("WindowEndTimestamp " + windowEndTimestamp);
        // Set timestamps exactly as in your original code
        Timestamp windowEnd = new Timestamp(windowEndTimestamp);
        result.timestamp = windowEnd;

        // The approach here is to convert the time to zoned in local timezone,
        // then convert the same instant (time without the timezone offset) to the utc zone
        ZonedDateTime zdt = ZonedDateTime.of(windowEnd.toLocalDateTime(), ZoneId.systemDefault());
        ZonedDateTime utc = zdt.withZoneSameInstant(ZoneId.of("UTC"));

        result.timestamp_tf_rounded_ntz = Timestamp.valueOf(
                result.timestamp.toLocalDateTime().atZone(ZoneId.of("UTC")).toLocalDateTime()
        );
        result.timestamp_tf_rounded_tz = result.timestamp;
        result.processing_timestamp = Timestamp.valueOf(LocalDateTime.now());

        out.collect(result);
        ctx.output(kafkaSideOutput, result);
        lastTradeState.update(result);

        // Clear the current window state
        activeWindowStates.remove(timestamp - WINDOW_OFFSET);

        // Schedule next timer
        ctx.timerService().registerProcessingTimeTimer(windowEndTimestamp + WINDOW_SIZE + WINDOW_OFFSET);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastTradeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastTrade", AggregatedTrade.class));

        activeWindowStates = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("state-overlap-tracker", Long.class, TradeAccumulator.class)
        );

        // Schedule the first timer aligned to the next minute boundary
        long now = System.currentTimeMillis();
        long nextWindow = ((now / WINDOW_SIZE) + 1) * WINDOW_SIZE;

        //getRuntimeContext().getProcessingTimeService().registerTimer(nextWindow, this);

        E9sLogger.logger.info("Initialized ContinuousTradeProcessor, first window will end at: " +
                new Timestamp(nextWindow));
    }
}