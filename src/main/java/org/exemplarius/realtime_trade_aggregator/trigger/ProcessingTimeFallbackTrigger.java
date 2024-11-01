package org.exemplarius.realtime_trade_aggregator.trigger;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.exemplarius.realtime_trade_aggregator.trade_transform.TradeUnit;
import org.exemplarius.realtime_trade_aggregator.utils.E9sLogger;

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
        ctx.registerEventTimeTimer(window.maxTimestamp());

        // TODO: I think this should be window.end, different window means different context, och  extra null context för att vi inte clearar processing time ordentiligt

        // Calculate and register next processing time timer just in case
        long nextMinute = window.getEnd() + intervalMs;//(ctx.getCurrentProcessingTime() / 60000 + 1) * 60000 + 4000;
        //System.out.println(nextMinute);
        //E9sLogger.logger.info("Max TIMESTAMP " + window.maxTimestamp());
        //System.out.println(window.maxTimestamp());
        //if (nextMinute <= window.getEnd()) {
            ctx.registerProcessingTimeTimer(nextMinute);
        //}
        //E9sLogger.logger.info("ELEMENT");
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
            return TriggerResult.CONTINUE;
        }
        boolean hasData = data.iterator().hasNext();

        // Schedule next processing timer
        long nextMinute = (time / 60000 + 1) * 60000 + intervalMs;
        //if (nextMinute <= window.getEnd()) {
            ctx.registerProcessingTimeTimer(nextMinute);
        //}
        hasDataState.clear();
        System.out.println("PROCESSING TIME has data? " + hasData);
        // Only fire if we have no data
        return TriggerResult.FIRE;
        //return hasData ? TriggerResult.CONTINUE : TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        E9sLogger.logger.info("Händer det saker här?");
//        if (time == window.maxTimestamp()) {
//            // Clear the state when window completes
//            System.out.println("EVENT TIME IS HERE!!!!!!!!");
//            ListState<Boolean> hasDataState = ctx.getPartitionedState(HAS_DATA_DESCRIPTOR);
//            hasDataState.clear();
//            return TriggerResult.FIRE;
//        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        E9sLogger.logger.info("CLEARING");

        ListState<Boolean> hasDataState = ctx.getPartitionedState(HAS_DATA_DESCRIPTOR);
        hasDataState.clear();
        ctx.deleteProcessingTimeTimer(window.getEnd() + intervalMs);
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        E9sLogger.logger.info("MERGING");
        // Register the timer for the merged window
        long next = (window.getEnd() / 60000 + 1) * 60000 + intervalMs;
        //ctx.registerProcessingTimeTimer(next);
    }
}