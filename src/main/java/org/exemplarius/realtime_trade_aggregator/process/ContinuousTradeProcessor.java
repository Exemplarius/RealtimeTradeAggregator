package org.exemplarius.realtime_trade_aggregator.process;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.exemplarius.realtime_trade_aggregator.trade_transform.AggregatedTrade;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeAccumulator;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeAggregateFunction;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeUnit;
import org.exemplarius.realtime_trade_aggregator.utils.E9sLogger;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ContinuousTradeProcessor extends KeyedProcessFunction<Boolean, TradeUnit, AggregatedTrade> {
    private static final long WINDOW_SIZE = 60 * 1000; // 1 minute in milliseconds

    private ValueState<AggregatedTrade> lastTradeState;
    private ValueState<TradeAccumulator> currentWindowState;



    @Override
    public void processElement(TradeUnit trade, Context ctx, Collector<AggregatedTrade> out) throws Exception {
        // Align to minute boundaries
        long eventTime = trade.timestamp.getTime();
        long currentWindowStart = (eventTime / WINDOW_SIZE) * WINDOW_SIZE;

        // Register timer for this window if not already registered
        long windowEnd = currentWindowStart + WINDOW_SIZE;
        ctx.timerService().registerProcessingTimeTimer(windowEnd);

        // Get or create accumulator for current window
        TradeAccumulator acc = currentWindowState.value();
        if (acc == null) {
            acc = new TradeAccumulator();
        }

        // Use existing aggregate function logic
        TradeAggregateFunction aggregator = new TradeAggregateFunction();
        acc = aggregator.add(trade, acc);
        currentWindowState.update(acc);

        E9sLogger.logger.info("Processed trade for window ending at: " + new Timestamp(windowEnd));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregatedTrade> out) throws Exception {
        TradeAccumulator acc = currentWindowState.value();
        AggregatedTrade lastTrade = lastTradeState.value();

        AggregatedTrade result = new AggregatedTrade();
        if (acc != null) {
            // We had trades in this window - use accumulated data
            result.trades = acc.trades;
            result.buys = acc.buys;
            result.sells = acc.sells;
            result.buyVolume = acc.buyVolume;
            result.sellVolume = acc.sellVolume;
            result.buyOpen = acc.buyOpen;
            result.sellOpen = acc.sellOpen;
            result.buyHigh = acc.buyHigh;
            result.sellHigh = acc.sellHigh;
            result.buyLow = acc.buyLow == Double.MAX_VALUE ? -1 : acc.buyLow;
            result.sellLow = acc.sellLow == Double.MAX_VALUE ? -1 : acc.sellLow;
            result.buyClose = acc.buyClose;
            result.sellClose = acc.sellClose;
            result.lastBuyTimestamp = acc.lastBuyTimestamp;
            result.lastSellTimestamp = acc.lastSellTimestamp;
            result.open = acc.open;
            result.high = acc.high;
            result.low = acc.low == Double.MAX_VALUE ? -1 : acc.low;
            result.close = acc.close;

            E9sLogger.logger.info("Processing window with " + acc.trades + " trades");
        } else if (lastTrade != null) {
            // Empty window - use last trade values
            result.trades = 0;
            result.buys = 0;
            result.sells = 0;
            result.buyVolume = 0;
            result.sellVolume = 0;
            // Maintain last known prices
            result.buyOpen = lastTrade.buyClose;
            result.sellOpen = lastTrade.sellClose;
            result.buyHigh = lastTrade.buyClose;
            result.sellHigh = lastTrade.sellClose;
            result.buyLow = lastTrade.buyClose;
            result.sellLow = lastTrade.sellClose;
            result.buyClose = lastTrade.buyClose;
            result.sellClose = lastTrade.sellClose;
            result.lastBuyTimestamp = lastTrade.lastBuyTimestamp;
            result.lastSellTimestamp = lastTrade.lastSellTimestamp;
            result.open = lastTrade.close;
            result.high = lastTrade.close;
            result.low = lastTrade.close;
            result.close = lastTrade.close;

            E9sLogger.logger.info("Processing empty window, using last known values");
        } else {
            E9sLogger.logger.info("No data available yet, skipping window");
            return;
        }

        // Set timestamps exactly as in your original code
        Timestamp windowEnd = new Timestamp(timestamp);
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
        lastTradeState.update(result);

        // Clear the current window state
        currentWindowState.clear();

        // Schedule next timer
        ctx.timerService().registerProcessingTimeTimer(timestamp + WINDOW_SIZE);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastTradeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastTrade", AggregatedTrade.class));
        currentWindowState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("currentWindow", TradeAccumulator.class));
        // Schedule the first timer aligned to the next minute boundary
        long now = System.currentTimeMillis();
        long nextWindow = ((now / WINDOW_SIZE) + 1) * WINDOW_SIZE;

        //getRuntimeContext().getProcessingTimeService().registerTimer(nextWindow, this);

        E9sLogger.logger.info("Initialized ContinuousTradeProcessor, first window will end at: " +
                new Timestamp(nextWindow));
    }
}