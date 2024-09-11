package org.exemplarius.realtime_trade_aggregator.jdbc_sink;

public class JdbcDatabaseConfig {
    private String hostname;
    private int port;
    private String database;
    private String username;
    private String password;

    public JdbcDatabaseConfig(String hostname, int port, String database, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
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

    public String getJdbcUrl(String engine) {
        switch (engine) {
            case "postgres":
                return String.format("jdbc:postgresql://%s:%d/%s", hostname, port, database);
            default:
                return "";
        }
    }
}