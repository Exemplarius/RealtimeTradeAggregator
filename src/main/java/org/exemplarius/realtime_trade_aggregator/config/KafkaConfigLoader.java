package org.exemplarius.realtime_trade_aggregator.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class KafkaConfigLoader {

    public static KafkaConfig LoadConfig() {
        Config conf = ConfigFactory.load();
        return new KafkaConfig(
                conf.getString("config.kafka.ingestion.server"),
                conf.getInt("config.kafka.ingestion.port"),
                new KafkaTopic(conf.getString("config.kafka.topic.trade_event"), conf.getString("config.kafka.topic.trade_aggregate_event"))
        );

    }

}
