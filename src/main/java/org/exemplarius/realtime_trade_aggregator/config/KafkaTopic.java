package org.exemplarius.realtime_trade_aggregator.config;


public class KafkaTopic {
    private final String tradeEvent;
    private final String tradeAggregateEvent;
    public KafkaTopic(String tradeEvent, String tradeAggregateEvent) {
        this.tradeEvent = tradeEvent;
        this.tradeAggregateEvent = tradeAggregateEvent;
    }
    public String getTradeEvent() {
        return tradeEvent;
    }
    public String getTradeAggregateEvent() {
        return tradeAggregateEvent;
    }
}