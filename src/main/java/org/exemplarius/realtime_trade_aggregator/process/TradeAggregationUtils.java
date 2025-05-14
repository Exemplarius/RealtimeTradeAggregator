package org.exemplarius.realtime_trade_aggregator.process;

import org.exemplarius.realtime_trade_aggregator.model.transform.AggregatedTrade;
import org.exemplarius.realtime_trade_aggregator.model.transform.TradeAccumulator;

public class TradeAggregationUtils {




    public static AggregatedTrade fromAccumulator(TradeAccumulator acc) {
        AggregatedTrade result = new AggregatedTrade();
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

        return result;
    }


    public static AggregatedTrade fromPreviousAggregatedTrade(AggregatedTrade previousAggregatedTrade) {


        AggregatedTrade result = new AggregatedTrade();
        result.trades = 0;
        result.buys = 0;
        result.sells = 0;
        result.buyVolume = 0;
        result.sellVolume = 0;
        // Maintain last known prices
        result.buyOpen = previousAggregatedTrade.buyClose;
        result.sellOpen = previousAggregatedTrade.sellClose;
        result.buyHigh = previousAggregatedTrade.buyClose;
        result.sellHigh = previousAggregatedTrade.sellClose;
        result.buyLow = previousAggregatedTrade.buyClose;
        result.sellLow = previousAggregatedTrade.sellClose;
        result.buyClose = previousAggregatedTrade.buyClose;
        result.sellClose = previousAggregatedTrade.sellClose;
        result.lastBuyTimestamp = previousAggregatedTrade.lastBuyTimestamp;
        result.lastSellTimestamp = previousAggregatedTrade.lastSellTimestamp;
        result.open = previousAggregatedTrade.close;
        result.high = previousAggregatedTrade.close;
        result.low = previousAggregatedTrade.close;
        result.close = previousAggregatedTrade.close;

        return result;
    }
}
