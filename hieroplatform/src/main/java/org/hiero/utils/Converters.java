package org.hiero.utils;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Conversion to and from doubles of various supported datatypes.
 */
public class Converters {
    private static final LocalDateTime baseTime = LocalDateTime.of(
            LocalDate.of(1970, 1, 1),
            LocalTime.of(0, 0));

    public static double toDouble(@Nonnull final LocalDateTime d) {
        Duration span = Duration.between(d, baseTime);
        return Converters.toDouble(span);
    }

    public static double toDouble(@Nonnull final Duration d) {
        return d.toNanos();
    }

    /**
     * Converts a date d to a double by taking the interval from a base date (Jan 1st 1970) and
     * converting this to a double
     * @param d input date
     * @return Span from base converted to a double
     */
    public static LocalDateTime toDate(final double d) {
        Duration span = toDuration(d);
        return baseTime.plus(span);
    }

    public static Duration toDuration(final double d) {
        return Duration.ofNanos((long)d);
    }
}
