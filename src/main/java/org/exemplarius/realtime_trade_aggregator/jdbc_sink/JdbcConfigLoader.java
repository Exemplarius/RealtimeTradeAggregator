package org.exemplarius.realtime_trade_aggregator.jdbc_sink;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class JdbcConfigLoader {

    public static JdbcDatabaseConfig LoadDatabaseConfig() {
        Config conf = ConfigFactory.load();
        return new JdbcDatabaseConfig(
            conf.getString("database.bitmex.postgres.hostname"),
            conf.getInt("database.bitmex.postgres.port"),
            conf.getString("database.bitmex.postgres.database"),
            conf.getString("database.bitmex.postgres.username"),
            conf.getString("database.bitmex.postgres.password")
        );
    }
}
