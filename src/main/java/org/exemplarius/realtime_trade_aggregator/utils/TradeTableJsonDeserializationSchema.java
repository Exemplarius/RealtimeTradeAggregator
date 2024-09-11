package org.exemplarius.realtime_trade_aggregator.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.exemplarius.realtime_trade_aggregator.trade_input.TradeTable;

import java.io.IOException;

public class TradeTableJsonDeserializationSchema implements DeserializationSchema<TradeTable> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TypeInformation<TradeTable> typeInfo = TypeInformation.of(TradeTable.class);

    @Override
    public TradeTable deserialize(byte[] message) throws IOException {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            return objectMapper.treeToValue(jsonNode, TradeTable.class);
        } catch (JsonProcessingException e) {
            // Log the error or handle it as needed
            System.err.println("Error deserializing message: " + new String(message));
            return null; // Return null for corrupt messages
        }
    }

    @Override
    public boolean isEndOfStream(TradeTable nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TradeTable> getProducedType() {
        return typeInfo;
    }
}