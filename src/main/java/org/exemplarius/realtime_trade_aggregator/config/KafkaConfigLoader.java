package org.exemplarius.realtime_trade_aggregator.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class KafkaConfigLoader {

    public static KafkaConfig LoadConfig() {
        Config conf = ConfigFactory.load();
        return new KafkaConfig(
                conf.getString("config.kafka.server"),
                conf.getInt("config.kafka.port")
        );

    }

}
