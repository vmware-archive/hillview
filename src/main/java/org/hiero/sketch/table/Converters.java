package org.hiero.sketch.table;

import java.time.Duration;
import java.util.Date;

/**
 * Conversion to and from doubles of various supported datatypes.
 */
public class Converters {
    public static double toDouble(Date d) {
        return d.getTime();
    }

    public static double toDouble(Duration d) {
        return d.toNanos();
    }

    public static Date toDate(double d) {
        return new Date((long)d);
    }

    public static Duration toDuration(double d) {
        return Duration.ofNanos((long)d);
    }
}
