package org.exemplarius.realtime_trade_aggregator.config;


public class KafkaTopic {
    private final String tradeEvent;
    private final String tradeAggregateEvent;
    private final String tradeAggregateBatch;
    public KafkaTopic(String tradeEvent, String tradeAggregateEvent, String tradeAggregateBatch) {
        this.tradeEvent = tradeEvent;
        this.tradeAggregateEvent = tradeAggregateEvent;
        this.tradeAggregateBatch = tradeAggregateBatch;
    }
    public String getTradeEvent() {
        return tradeEvent;
    }
    public String getTradeAggregateEvent() {
        return tradeAggregateEvent;
    }
}