package org.exemplarius.realtime_trade_aggregator.trade_transform;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.exemplarius.realtime_trade_aggregator.model.transform.AggregatedTrade;
import org.exemplarius.realtime_trade_aggregator.model.transform.TradeAccumulator;
import org.exemplarius.realtime_trade_aggregator.utils.E9sLogger;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
public class EmptyAwareTradeWindowFunction extends ProcessWindowFunction<TradeAccumulator, AggregatedTrade, Boolean, TimeWindow> {
    private static final MapStateDescriptor<String, AggregatedTrade> LAST_TRADE_STATE_DESC =
            new MapStateDescriptor<>("lastTradeState", String.class, AggregatedTrade.class);

    @Override
    public void process(Boolean key, Context context, Iterable<TradeAccumulator> elements, Collector<AggregatedTrade> out) {
        MapState<String, AggregatedTrade> lastTradeState = context.globalState().getMapState(LAST_TRADE_STATE_DESC);

        try {
            if (!elements.iterator().hasNext()) {
                E9sLogger.logger.info("No records available for window - using last known values");
                handleEmptyWindow(lastTradeState, context, out);
                return;
            }

            // Process normal window with data
            TradeAccumulator acc = elements.iterator().next();
            AggregatedTrade result = createAggregatedTrade(acc, context);

            // Store this result for future empty windows
            lastTradeState.put("lastTrade", result);

            out.collect(result);

        } catch (Exception e) {
            E9sLogger.logger.error("Error processing window: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void handleEmptyWindow(MapState<String, AggregatedTrade> lastTradeState,
                                   Context context,
                                   Collector<AggregatedTrade> out) throws Exception {

        AggregatedTrade lastTrade = lastTradeState.get("lastTrade");

        if (lastTrade == null) {
            E9sLogger.logger.warn("No previous trade data available for empty window");
            return;
        }

        // Create new trade with previous close values but current timestamp
        AggregatedTrade emptyTrade = new AggregatedTrade();

        // Copy all close values from last trade
        emptyTrade.trades = 0;  // Set to 0 as no actual trades
        emptyTrade.buys = 0;
        emptyTrade.sells = 0;
        emptyTrade.buyVolume = 0.0;
        emptyTrade.sellVolume = 0.0;

        // Use previous close values for all OHLC fields
        emptyTrade.buyOpen = lastTrade.buyClose;
        emptyTrade.buyHigh = lastTrade.buyClose;
        emptyTrade.buyLow = lastTrade.buyClose;
        emptyTrade.buyClose = lastTrade.buyClose;

        emptyTrade.sellOpen = lastTrade.sellClose;
        emptyTrade.sellHigh = lastTrade.sellClose;
        emptyTrade.sellLow = lastTrade.sellClose;
        emptyTrade.sellClose = lastTrade.sellClose;

        emptyTrade.open = lastTrade.close;
        emptyTrade.high = lastTrade.close;
        emptyTrade.low = lastTrade.close;
        emptyTrade.close = lastTrade.close;

        // Keep last timestamps from previous trade
        emptyTrade.lastBuyTimestamp = lastTrade.lastBuyTimestamp;
        emptyTrade.lastSellTimestamp = lastTrade.lastSellTimestamp;

        // Set timestamps for current window
        setWindowTimestamps(emptyTrade, context);

        out.collect(emptyTrade);
    }

    private AggregatedTrade createAggregatedTrade(TradeAccumulator acc, Context context) {
        AggregatedTrade result = new AggregatedTrade();

        // Transfer data from accumulator to result
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

        setWindowTimestamps(result, context);

        return result;
    }

    private void setWindowTimestamps(AggregatedTrade result, Context context) {
        Timestamp windowEnd = new Timestamp(context.window().getEnd());
        result.timestamp = windowEnd;

        // Convert to UTC while preserving the instant
        ZonedDateTime zdt = ZonedDateTime.of(windowEnd.toLocalDateTime(), ZoneId.systemDefault());
        ZonedDateTime utc = zdt.withZoneSameInstant(ZoneId.of("UTC"));

        result.timestamp_tf_rounded_ntz = Timestamp.valueOf(result.timestamp.toLocalDateTime()
                .atZone(ZoneId.of("UTC")).toLocalDateTime());
        result.timestamp_tf_rounded_tz = result.timestamp;
        result.processing_timestamp = Timestamp.valueOf(LocalDateTime.now());
    }
}