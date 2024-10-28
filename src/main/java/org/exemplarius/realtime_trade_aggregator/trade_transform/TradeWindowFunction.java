package org.exemplarius.realtime_trade_aggregator.trade_transform;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.exemplarius.realtime_trade_aggregator.utils.E9sLogger;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TradeWindowFunction extends ProcessWindowFunction<TradeAccumulator, AggregatedTrade, Boolean, TimeWindow> {

    /**
     * This function will run for each element inside the window
     * @param key
     * @param context
     * @param elements
     * @param out
     */
    @Override
    public void process(Boolean key, Context context, Iterable<TradeAccumulator> elements, Collector<AggregatedTrade> out) {

        System.out.println("HELLO");
        E9sLogger.logger.info("PROCESSING");
        if (!elements.iterator().hasNext()) {
            E9sLogger.logger.info("No records available");
        }


        TradeAccumulator acc = elements.iterator().next();

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
        result.sellLow = acc.sellLow == Double.MAX_VALUE ? -1: acc.sellLow;
        result.buyClose = acc.buyClose;
        result.sellClose = acc.sellClose;
        result.lastBuyTimestamp = acc.lastBuyTimestamp;
        result.lastSellTimestamp = acc.lastSellTimestamp;
        result.open = acc.open;
        result.high = acc.high;
        result.low = acc.low == Double.MAX_VALUE ? -1: acc.low;
        result.close = acc.close;

        Timestamp windowEnd = new Timestamp(context.window().getEnd());
        result.timestamp = windowEnd;

        // The approach here is to convert the time to zoned in local timezone, then convert the same instant (time without the timezone offset) to the utc zone
        ZonedDateTime zdt = ZonedDateTime.of(windowEnd.toLocalDateTime(), ZoneId.systemDefault());
        ZonedDateTime utc = zdt.withZoneSameInstant(ZoneId.of("UTC"));
        Timestamp timestampInUtc = Timestamp.valueOf(utc.toLocalDateTime());

        result.timestamp_tf_rounded_ntz = Timestamp.valueOf(result.timestamp.toLocalDateTime().atZone(ZoneId.of("UTC")).toLocalDateTime());
        result.timestamp_tf_rounded_tz = result.timestamp;
        result.processing_timestamp = Timestamp.valueOf(LocalDateTime.now());
        out.collect(result);
    }
}
