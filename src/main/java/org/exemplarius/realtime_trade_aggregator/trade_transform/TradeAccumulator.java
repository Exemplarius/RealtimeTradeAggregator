package org.exemplarius.realtime_trade_aggregator.trade_transform;

import java.sql.Timestamp;

public class TradeAccumulator {
    public long trades = 0;
    public long buys = 0;
    public long sells = 0;
    public double buyVolume = 0;
    public double sellVolume = 0;
    public double buyOpen = 0;
    public double sellOpen = 0;
    public double buyHigh = Double.MIN_VALUE;
    public double sellHigh = Double.MIN_VALUE;
    public double buyLow = Double.MAX_VALUE;
    public double sellLow = Double.MAX_VALUE;
    public double buyClose = 0;
    public double sellClose = 0;
    public Timestamp lastBuyTimestamp = new Timestamp(0);
    public Timestamp lastSellTimestamp = new Timestamp(0);
    public double open = 0;
    public double high = Double.MIN_VALUE;
    public double low = Double.MAX_VALUE;
    public double close = 0;
}