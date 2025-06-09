package org.exemplarius.realtime_trade_aggregator.model.transform;

import java.sql.Timestamp;

public class TradeAccumulator {
    public long trades = 0;
    public long buys = 0;
    public long sells = 0;
    public double buyVolume = 0;
    public double sellVolume = 0;
    public double buyOpen = -1;
    public double sellOpen = -1;
    public double buyHigh = -1;//Double.MIN_VALUE;
    public double sellHigh = -1; //Double.MIN_VALUE;
    public double buyLow =  -1; //Double.MAX_VALUE;
    public double sellLow = -1; //Double.MAX_VALUE;
    public double buyClose = -1;
    public double sellClose = -1;
    public Timestamp lastBuyTimestamp = null; //new Timestamp(0);
    public Timestamp lastSellTimestamp = null; //new Timestamp(0);
    public double open = -1;
    public double high = -1; //Double.MIN_VALUE;
    public double low =  -1;
    public double close = -1;
}