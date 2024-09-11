package org.exemplarius.realtime_trade_aggregator.trade_transform;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class TradeWindowFunction extends ProcessWindowFunction<TradeAccumulator, AggregatedTrade, Boolean, TimeWindow> {
    @Override
    public void process(Boolean key, Context context, Iterable<TradeAccumulator> elements, Collector<AggregatedTrade> out) {
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
        result.buyLow = acc.buyLow;
        result.sellLow = acc.sellLow;
        result.buyClose = acc.buyClose;
        result.sellClose = acc.sellClose;
        result.lastBuyTimestamp = acc.lastBuyTimestamp;
        result.lastSellTimestamp = acc.lastSellTimestamp;
        result.open = acc.open;
        result.high = acc.high;
        result.low = acc.low;
        result.close = acc.close;
        result.windowEnd = new Timestamp(context.window().getEnd());
        out.collect(result);
    }
}
