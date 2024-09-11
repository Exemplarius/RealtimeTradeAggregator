package org.exemplarius.realtime_trade_aggregator.trade_input;

public class KafkaTradeMessage {
    private String key;
    private TradeTable value;

    // Constructors
    public KafkaTradeMessage() {}

    public KafkaTradeMessage(String key, TradeTable value) {
        this.key = key;
        this.value = value;
    }

    // Getters and Setters
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public TradeTable getValue() {
        return value;
    }

    public void setValue(TradeTable value) {
        this.value = value;
    }
}
