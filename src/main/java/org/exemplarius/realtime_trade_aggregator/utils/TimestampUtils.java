package org.exemplarius.realtime_trade_aggregator.utils;

import java.sql.Timestamp;

public class TimestampUtils {


    public static  <T extends Comparable<T>> T greater ( T ta, T tb) {
        if (ta.compareTo(tb) > 0) {
            return ta;
        }
        return tb;
    }

}
