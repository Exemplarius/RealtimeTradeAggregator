package org.exemplarius.realtime_trade_aggregator.config;
public class KafkaConfig {


    private String server;
    private int port;

    public KafkaConfig(String server, int port) {
        this.server = server;
        this.port = port;
    }


    public String getServer() {
        return server;
    }

    public int getPort() {
        return port;
    }
}