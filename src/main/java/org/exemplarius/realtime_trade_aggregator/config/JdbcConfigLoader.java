package org.exemplarius.realtime_trade_aggregator.config;

import java.net.URI;

public class JdbcConfigLoader {

    // The DB is declared as ONE connection-string env var (E9S_MARKET_DATABASE_URL) plus a separate
    // schema var (MARKET_SCHEMA), instead of split host/port/user/password. Both required.
    public static JdbcDatabaseConfig LoadConfig() {
        URI uri = URI.create(requireEnv("E9S_MARKET_DATABASE_URL"));
        String schema = requireEnv("MARKET_SCHEMA");

        // java.net.URI.getHost/getUserInfo/getPort return null/-1 when the host contains an underscore
        // (e.g. the docker service name `candle_database`), so parse the authority ([user[:pass]@]host[:port])
        // ourselves to stay robust to underscore hosts.
        String authority = uri.getAuthority() == null ? "" : uri.getAuthority();
        String userInfoPart = "";
        String hostPort = authority;
        int at = authority.indexOf('@');
        if (at >= 0) {
            userInfoPart = authority.substring(0, at);
            hostPort = authority.substring(at + 1);
        }
        String[] creds = userInfoPart.split(":", 2);
        String[] hp = hostPort.split(":", 2);
        int port = (hp.length > 1 && !hp[1].isEmpty()) ? Integer.parseInt(hp[1]) : 5432;
        String path = uri.getPath() == null ? "" : uri.getPath();
        String database = path.startsWith("/") ? path.substring(1) : path;

        return new JdbcDatabaseConfig(
                hp[0],
                port,
                database,
                creds.length > 0 ? creds[0] : "",
                creds.length > 1 ? creds[1] : "",
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
