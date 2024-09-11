package org.exemplarius.realtime_trade_aggregator;

import java.util.List;

public class TradeTable {
    private String table;
    private String action;
    private List<Trade> data;

    // Constructors
    public TradeTable() {}

    public TradeTable(String table, String action, List<Trade> data) {
        this.table = table;
        this.action = action;
        this.data = data;
    }

    // Getters and Setters
    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public List<Trade> getData() {
        return data;
    }

    public void setData(List<Trade> data) {
        this.data = data;
    }
}