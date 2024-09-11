
# RealtimeTradeAggregator
A Flink application for realtime Bitmex trade stream transformations

This is a template application that originally reads Exemplarius kafka topics and aggregates
the data and insert it to postgres

The data output should not be delayed more than 3 seconds, the aggregation window is set to 1 minute, the
watermark is set to 3 seconds.