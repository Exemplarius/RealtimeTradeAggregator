package org.exemplarius.realtime_trade_aggregator.trade_transform;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.exemplarius.realtime_trade_aggregator.model.transform.TradeAccumulator;
import org.exemplarius.realtime_trade_aggregator.model.transform.TradeUnit;


public class TradeAggregateFunction implements AggregateFunction<TradeUnit, TradeAccumulator, TradeAccumulator> {
    @Override
    public TradeAccumulator createAccumulator() {
        return new TradeAccumulator();
    }

    @Override
    public TradeAccumulator add(TradeUnit trade, TradeAccumulator acc) {
        acc.trades++;
        if ("Buy".equals(trade.side)) {
            acc.buys++;
            acc.buyVolume += trade.size;
            acc.buyHigh = Math.max(acc.buyHigh, trade.price);
            acc.buyLow = Math.min(acc.buyLow, trade.price);
            if (acc.buyOpen == 0) acc.buyOpen = trade.price;
            acc.buyClose = trade.price;
            acc.lastBuyTimestamp = trade.timestamp;
        } else {
            acc.sells++;
            acc.sellVolume += trade.size;
            acc.sellHigh = Math.max(acc.sellHigh, trade.price);
            acc.sellLow = acc.sellLow > 0 ? Math.min(acc.sellLow, trade.price): trade.price;
            if (acc.sellOpen == 0) acc.sellOpen = trade.price;
            acc.sellClose = trade.price;
            acc.lastSellTimestamp = trade.timestamp;
        }
        if (acc.open == 0) acc.open = trade.price;
        acc.high = Math.max(acc.high, trade.price);
        acc.low = Math.min(acc.low, trade.price);
        acc.close = trade.price;
        return acc;
    }

    @Override
    public TradeAccumulator getResult(TradeAccumulator acc) {
        return acc;
    }

    @Override
    public TradeAccumulator merge(TradeAccumulator acc1, TradeAccumulator acc2) {
        // Implement merge logic if needed for parallel processing
        return acc1;
    }

}