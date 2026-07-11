package org.exemplarius.realtime_trade_aggregator.config;

public class JdbcDatabaseConfig {
    private String hostname;
    private int port;
    private String database;
    private String username;
    private String password;
    private String schema;

    public JdbcDatabaseConfig(String hostname, int port, String database, String username, String password, String schema) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.schema = schema;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getJdbcUrl(String engine) {
        switch (engine) {
            // trade_volume tables are unqualified, so currentSchema routes writes to the market schema.
            case "postgres":
                return String.format("jdbc:postgresql://%s:%d/%s?currentSchema=%s", hostname, port, database, schema);
            default:
                return "";
        }
    }
}