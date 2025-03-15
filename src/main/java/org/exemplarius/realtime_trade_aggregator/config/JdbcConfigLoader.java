package org.exemplarius.realtime_trade_aggregator.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class JdbcConfigLoader {

    public static JdbcDatabaseConfig LoadConfig() {
        Config conf = ConfigFactory.load();

        return new JdbcDatabaseConfig(
                System.getenv("POSTGRES_HOST"),
                Integer.parseInt(System.getenv("POSTGRES_PORT")),
                conf.getString("config.database.postgres.database"),
                System.getenv("POSTGRES_USER"),
                System.getenv("POSTGRES_PASS")
        );
    }
}
