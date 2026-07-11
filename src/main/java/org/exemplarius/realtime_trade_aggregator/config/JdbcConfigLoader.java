package org.exemplarius.realtime_trade_aggregator.config;

import java.net.URI;

public class JdbcConfigLoader {

    // The DB is declared as ONE connection-string env var (E9S_MARKET_DATABASE_URL) plus a separate
    // schema var (MARKET_SCHEMA), instead of split host/port/user/password. Both required.
    public static JdbcDatabaseConfig LoadConfig() {
        URI uri = URI.create(requireEnv("E9S_MARKET_DATABASE_URL"));
        String schema = requireEnv("MARKET_SCHEMA");

        String[] userInfo = (uri.getUserInfo() == null ? "" : uri.getUserInfo()).split(":", 2);
        int port = uri.getPort() == -1 ? 5432 : uri.getPort();
        String path = uri.getPath();
        String database = path.startsWith("/") ? path.substring(1) : path;

        return new JdbcDatabaseConfig(
                uri.getHost(),
                port,
                database,
                userInfo.length > 0 ? userInfo[0] : "",
                userInfo.length > 1 ? userInfo[1] : "",
                schema
        );
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(name + " environment variable is required");
        }
        return value;
    }
}
