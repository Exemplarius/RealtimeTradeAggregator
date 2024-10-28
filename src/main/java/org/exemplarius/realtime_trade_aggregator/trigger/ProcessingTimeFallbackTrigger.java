package org.exemplarius.realtime_trade_aggregator.trigger;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeUnit;
public class ProcessingTimeFallbackTrigger extends Trigger<TradeUnit, TimeWindow> {

    private final long intervalMs;
    private static final ListStateDescriptor<Boolean> HAS_DATA_DESCRIPTOR = new ListStateDescriptor<>("hasData", Boolean.class); // Value state descriptor?
    public ProcessingTimeFallbackTrigger(Time interval) {
        this.intervalMs = interval.toMilliseconds();

    }
    @Override
    public TriggerResult onElement(TradeUnit element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        // Mark that we have data for this window
        ListState<Boolean> hasDataState = ctx.getPartitionedState(HAS_DATA_DESCRIPTOR);
        hasDataState.clear();
        hasDataState.add(true);

        // Register event time timer for window end
        ctx.registerEventTimeTimer(window.maxTimestamp());// TODO: I think this should be window.end

        // Calculate and register next processing time timer just in case
        long nextMinute = (ctx.getCurrentProcessingTime() / 60000 + 1) * 60000 + 4000;
        //if (nextMinute <= window.getEnd()) {
            ctx.registerProcessingTimeTimer(nextMinute);
        //}

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        // Only fire if we haven't received any data
        ListState<Boolean> hasDataState = ctx.getPartitionedState(HAS_DATA_DESCRIPTOR);
        System.out.println("Got state");
        Iterable<Boolean> data = hasDataState.get();
        if (data == null) {
            System.out.println("WHY IS IT NULL?");
            return TriggerResult.FIRE;
        }
        boolean hasData = data.iterator().hasNext();

        // Schedule next processing timer
        long nextMinute = (time / 60000 + 1) * 60000 + 4000;
        //if (nextMinute <= window.getEnd()) {
            ctx.registerProcessingTimeTimer(nextMinute);
        //}
        System.out.println("PROCESSING TIME hast data? " + hasData);
        // Only fire if we have no data
        return hasData ? TriggerResult.CONTINUE : TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()) {
            // Clear the state when window completes
            System.out.println("HERE!!!!!!!!");
            ListState<Boolean> hasDataState = ctx.getPartitionedState(HAS_DATA_DESCRIPTOR);
            hasDataState.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ListState<Boolean> hasDataState = ctx.getPartitionedState(HAS_DATA_DESCRIPTOR);
        hasDataState.clear();
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}