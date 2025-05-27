package org.exemplarius.realtime_trade_aggregator.model.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.exemplarius.realtime_trade_aggregator.model.transform.AggregatedTrade;

public class AggregatedTradeSerializationSchema implements SerializationSchema<AggregatedTrade> {


    @Override
    public byte[] serialize(AggregatedTrade aggregatedTrade) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsBytes(aggregatedTrade);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
