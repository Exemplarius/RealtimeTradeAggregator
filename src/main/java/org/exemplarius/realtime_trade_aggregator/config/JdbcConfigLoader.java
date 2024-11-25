package org.exemplarius.realtime_trade_aggregator.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class JdbcConfigLoader {

    public static JdbcDatabaseConfig LoadConfig() {
        Config conf = ConfigFactory.load();
        return new JdbcDatabaseConfig(
            conf.getString("config.database.postgres.hostname"),
            conf.getInt("config.database.postgres.port"),
            conf.getString("config.database.postgres.database"),
            conf.getString("config.database.postgres.username"),
            conf.getString("config.database.postgres.password")
        );
    }
}
