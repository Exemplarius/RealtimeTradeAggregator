package org.exemplarius.realtime_trade_aggregator.config;
public class KafkaConfig {

    private String server;
    private int port;
    private KafkaTopic topic;

    public KafkaConfig(String server, int port, KafkaTopic topic) {
        this.server = server;
        this.port = port;
        this.topic = topic;
    }


    public String getServer() {
        return server;
    }

    public int getPort() {
        return port;
    }


    public KafkaTopic getTopic() {
        return topic;
    }
}