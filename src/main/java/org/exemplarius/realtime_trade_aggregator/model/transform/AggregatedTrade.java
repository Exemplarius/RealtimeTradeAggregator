package org.exemplarius.realtime_trade_aggregator.model.transform;

import java.sql.Timestamp;

public class AggregatedTrade {
    public long trades;
    public long buys;
    public long sells;
    public double buyVolume;
    public double sellVolume;
    public double buyOpen;
    public double sellOpen;
    public double buyHigh;
    public double sellHigh;
    public double buyLow;
    public double sellLow;
    public double buyClose;
    public double sellClose;
    public Timestamp lastBuyTimestamp;
    public Timestamp lastSellTimestamp;
    public double open;
    public double high;
    public double low;
    public double close;
    public Timestamp timestamp;

    public Timestamp timestamp_tf_rounded_ntz;

    public Timestamp timestamp_tf_rounded_tz;

    public Timestamp processing_timestamp;
    // Constructor and toString method omitted for brevity


    @Override
    public String toString() {
        return "AggregatedTrade{" +
                "trades=" + trades +
                ", buys=" + buys +
                ", sells=" + sells +
                ", buyVolume=" + String.format("%.2f", buyVolume) +
                ", sellVolume=" + String.format("%.2f", sellVolume) +
                ", buyOpen=" + String.format("%.2f", buyOpen) +
                ", sellOpen=" + String.format("%.2f", sellOpen) +
                ", buyHigh=" + String.format("%.2f", buyHigh) +
                ", sellHigh=" + String.format("%.2f", sellHigh) +
                ", buyLow=" + String.format("%.2f", buyLow) +
                ", sellLow=" + String.format("%.2f", sellLow) +
                ", buyClose=" + String.format("%.2f", buyClose) +
                ", sellClose=" + String.format("%.2f", sellClose) +
                ", lastBuyTimestamp=" + (lastBuyTimestamp != null ? lastBuyTimestamp.toString() : "null") +
                ", lastSellTimestamp=" + (lastSellTimestamp != null ? lastSellTimestamp.toString() : "null") +
                ", open=" + String.format("%.2f", open) +
                ", high=" + String.format("%.2f", high) +
                ", low=" + String.format("%.2f", low) +
                ", close=" + String.format("%.2f", close) +
                ", windowEnd=" + (timestamp != null ? timestamp.toString() : "null") +
                '}';
    }
}