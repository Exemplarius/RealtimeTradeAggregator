package org.exemplarius.realtime_trade_aggregator;
import java.sql.Timestamp;

public class Trade {
    private Timestamp timestamp;
    private String symbol;
    private String side;
    private int size;
    private double price;
    private String tickDirection;
    private String trdMatchID;
    private int grossValue;
    private double homeNotional;
    private double foreignNotional;
    private String trdType;

    // Constructors
    public Trade() {}

    public Trade(Timestamp timestamp, String symbol, String side, int size, double price, String tickDirection,
                 String trdMatchID, int grossValue, double homeNotional, double foreignNotional, String trdType) {
        this.timestamp = timestamp;
        this.symbol = symbol;
        this.side = side;
        this.size = size;
        this.price = price;
        this.tickDirection = tickDirection;
        this.trdMatchID = trdMatchID;
        this.grossValue = grossValue;
        this.homeNotional = homeNotional;
        this.foreignNotional = foreignNotional;
        this.trdType = trdType;
    }

    // Getters and Setters
    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getTickDirection() {
        return tickDirection;
    }

    public void setTickDirection(String tickDirection) {
        this.tickDirection = tickDirection;
    }

    public String getTrdMatchID() {
        return trdMatchID;
    }

    public void setTrdMatchID(String trdMatchID) {
        this.trdMatchID = trdMatchID;
    }

    public int getGrossValue() {
        return grossValue;
    }

    public void setGrossValue(int grossValue) {
        this.grossValue = grossValue;
    }

    public double getHomeNotional() {
        return homeNotional;
    }

    public void setHomeNotional(double homeNotional) {
        this.homeNotional = homeNotional;
    }

    public double getForeignNotional() {
        return foreignNotional;
    }

    public void setForeignNotional(double foreignNotional) {
        this.foreignNotional = foreignNotional;
    }

    public String getTrdType() {
        return trdType;
    }

    public void setTrdType(String trdType) {
        this.trdType = trdType;
    }
}
