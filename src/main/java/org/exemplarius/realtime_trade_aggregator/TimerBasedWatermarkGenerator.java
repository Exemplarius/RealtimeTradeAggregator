package org.exemplarius.realtime_trade_aggregator;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.time.Duration;

public class TimerBasedWatermarkGenerator implements WatermarkGenerator<TradeTable> {

    private final long maxOutOfOrderness;
    private long currentMaxTimestamp;
    private final long watermarkInterval;
    private long lastWatermarkTimestamp;

    public TimerBasedWatermarkGenerator(Duration maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness.toMillis();
        this.watermarkInterval = maxOutOfOrderness.toMillis();
        this.currentMaxTimestamp = Long.MIN_VALUE;
        this.lastWatermarkTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void onEvent(TradeTable event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        long potentialWatermark = currentMaxTimestamp - maxOutOfOrderness;
        if (potentialWatermark >= lastWatermarkTimestamp + watermarkInterval) {
            output.emitWatermark(new Watermark(potentialWatermark));
            lastWatermarkTimestamp = potentialWatermark;
        }
    }
}