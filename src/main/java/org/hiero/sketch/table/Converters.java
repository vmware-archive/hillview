package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.util.Date;

/**
 * Conversion to and from doubles of various supported datatypes.
 */
public class Converters {
    public static double toDouble(@NonNull final Date d) {
        return d.getTime();
    }

    public static double toDouble(@NonNull final Duration d) {
        return d.toNanos();
    }

    public static Date toDate(final double d) {
        return new Date((long)d);
    }

    public static Duration toDuration(final double d) {
        return Duration.ofNanos((long)d);
    }
}
