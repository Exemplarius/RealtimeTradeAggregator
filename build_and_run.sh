#!/bin/bash

# Load all variables from .env file into the current shell
set -a
source .env
set +a

# Run 
/usr/lib/jvm/java-11-openjdk/bin/java -cp target/RealtimeTradeAggregator-1.0-SNAPSHOT-jar-with-dependencies.jar org.exemplarius.realtime_trade_aggregator.Main