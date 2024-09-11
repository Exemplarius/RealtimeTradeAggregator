package org.exemplarius.realtime_trade_aggregator.trade_transform;

import java.sql.Timestamp;

public class TradeUnit {
    public Timestamp timestamp;
    public String side;
    public double size;
    public double price;

    public TradeUnit(Timestamp timestamp, String side, double size, double price) {
        this.timestamp = timestamp;
        this.side = side;
        this.size = size;
        this.price = price;
    }
}