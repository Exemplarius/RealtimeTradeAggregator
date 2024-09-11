package org.exemplarius.realtime_trade_aggregator.jdbc_sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.exemplarius.realtime_trade_aggregator.trade_transform.AggregatedTrade;

import java.sql.PreparedStatement;

public class JdbcDatabaseSink {

    public static SinkFunction<AggregatedTrade> Elva(String tableName) {
        JdbcDatabaseConfig config = JdbcConfigLoader.LoadDatabaseConfig();
        SinkFunction<AggregatedTrade> sink =
            JdbcSink.sink(
                    "insert into " + tableName + " (trades, buys, sells, buy_volume, sell_volume, buy_open, sell_open, buy_high, sell_high, buy_low, sell_low, buy_close, sell_close, last_buy_timestamp, last_sell_timestamp, open, high, low, close, timestamp, timestamp_tf_rounded_ntz, timestamp_tf_rounded_tz, processing_timestamp) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (PreparedStatement statement, AggregatedTrade tradeData) -> {
                        statement.setLong(1, tradeData.trades);
                        statement.setLong(2, tradeData.buys);
                        statement.setLong(3, tradeData.sells);
                        statement.setDouble(4, tradeData.buyVolume);
                        statement.setDouble(5, tradeData.sellVolume);
                        statement.setDouble(6, tradeData.buyOpen);
                        statement.setDouble(7, tradeData.sellOpen);
                        statement.setDouble(8, tradeData.buyHigh);
                        statement.setDouble(9, tradeData.sellHigh);
                        statement.setDouble(10, tradeData.buyLow);
                        statement.setDouble(11, tradeData.sellLow);
                        statement.setDouble(12, tradeData.buyClose);
                        statement.setDouble(13, tradeData.sellClose);
                        statement.setTimestamp(14, tradeData.lastBuyTimestamp);
                        statement.setTimestamp(15, tradeData.lastSellTimestamp);
                        statement.setDouble(16, tradeData.open);
                        statement.setDouble(17, tradeData.high);
                        statement.setDouble(18, tradeData.low);
                        statement.setDouble(19, tradeData.close);
                        statement.setTimestamp(20, tradeData.timestamp);
                        statement.setTimestamp(21, tradeData.timestamp_tf_rounded_ntz);
                        statement.setTimestamp(22, tradeData.timestamp_tf_rounded_tz);
                        statement.setTimestamp(23, tradeData.processing_timestamp);
                    },
                    JdbcExecutionOptions.builder()
                            .withBatchSize(1000)
                            .withBatchIntervalMs(200)
                            .withMaxRetries(5)
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(config.getJdbcUrl("postgres"))//"jdbc:postgresql://dbhost:5432/postgresdb")
                            .withDriverName("org.postgresql.Driver")
                            .withUsername(config.getUsername())
                            .withPassword(config.getPassword())
                            .build()
            );
        return sink;
    }

}
