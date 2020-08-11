/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.utils;

import javax.annotation.Nullable;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Conversion to and from doubles of various supported datatypes.
 */
public class Converters {
    public static final long NANOS_TO_SECONDS = 1_000_000_000;
    public static final int NANOS_TO_MILLIS = 1_000_000;
    public static final int SECONDS_TO_DAY = 24 * 3600;

    private static final LocalDateTime baseLocalTime = LocalDateTime.of(
            LocalDate.of(1970, 1, 1),
            LocalTime.of(0, 0));
    private static final Instant baseTime = baseLocalTime.toInstant(ZoneOffset.UTC);

    public static double toDouble(final Instant d) {
        Duration span = Duration.between(baseTime, d);
        return Converters.toDouble(span);
    }

    public static double toDouble(final Duration d) {
        return d.toMillis();
    }

    // TODO: these representations are too coarse to support sub-millisecond timestamps

    /**
     * Converts a date d to a double by taking the interval from a base date (Jan 1st 1970) and
     * converting this to a double.
     * @param d input date.
     * @return Span from base converted to a double.
     */
    public static Instant toDate(final double d) {
        Duration span = toDuration(d);
        return baseTime.plus(span);
    }

    @Nullable
    public static Instant toDate(@Nullable final Double d) {
        if (d == null)
            return null;
        return toDate((double)d);
    }

    public static double toDouble(LocalDateTime date) {
        Duration span = Duration.between(baseLocalTime, date);
        return Converters.toDouble(span);
    }

    public static LocalDateTime toLocalDate(double value) {
        Duration span = toDuration(value);
        return baseLocalTime.plus(span);
    }

    public static Duration toDuration(final double d) {
        return Duration.ofMillis(toLong(d));
    }

    /**
     * Casts a Nullable pointer to a NonNullable one.  Throws if pointer is null.
     * @param data  Nullable pointer.
     * @param <T>   Type of nullable pointer.
     * @return      The same pointer, but never null.
     */
    public static <T> T checkNull(@Nullable T data) {
        if (data == null)
            throw new NullPointerException("Value should not be null");
        return data;
    }

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS")
            .withLocale(Locale.US)
            .withZone(ZoneId.systemDefault());

    static String stripSuffix(String dateTime) {
        if (dateTime.endsWith(".000"))
            dateTime = dateTime.substring(0, dateTime.length() - 4);
        if (dateTime.endsWith(" 00:00:00"))
            return dateTime.substring(0, dateTime.length() - 9);
        if (dateTime.endsWith(":00"))
            return dateTime.substring(0, dateTime.length() - 3);
        return dateTime;
    }

    /**
     * Convert a date to a string.  Must be the same algorithm as the
     * one used in JavaScript to convert dates.
     */
    @Nullable
    public static String toString(@Nullable Instant d) {
        if (d == null)
            return null;
        String s = formatter.format(d);
        return stripSuffix(s);
    }

    @Nullable
    public static String toString(@Nullable LocalTime t) {
        if (t == null)
            return null;
        return t.toString();
    }

    @Nullable
    public static String toString(@Nullable LocalDateTime t) {
        if (t == null)
            return null;
        String s = formatter.format(t);
        return stripSuffix(s);
    }

    /**
     * Compare two strings. In Hillview the null string is the greatest value.
     * @param left   Left string to compare.
     * @param right  Right string to compare.
     * @return       A comparison value: -1, 0 or 1.
     */
    public static int compareStrings(@Nullable String left, @Nullable String right) {
        if (left == null && right == null) {
            return 0;
        } else if (left == null) {
            return 1;
        } else if (right == null) {
            return -1;
        } else {
            return left.compareTo(right);
        }
    }

    @Nullable
    public static String min(@Nullable String left, @Nullable String right) {
        int x = compareStrings(left, right);
        if (x <= 0)
            return left;
        return right;
    }

    @Nullable
    public static String max(@Nullable String left, @Nullable String right) {
        int x = compareStrings(left, right);
        if (x >= 0)
            return left;
        return right;
    }

    public static long toLong(double value) {
        if (value < Long.MIN_VALUE || value > Long.MAX_VALUE)
            throw new RuntimeException("Cannot convert to long " + value);
        return (long)value;
    }

    public static int toInt(double value) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
            throw new RuntimeException("Cannot convert to int " + value);
        return (int)value;
    }

    public static LocalTime toTime(double value) {
        return LocalTime.ofNanoOfDay(Converters.toLong(value * NANOS_TO_MILLIS));
    }

    public static double toDouble(LocalTime time) {
        return (double)(time.toNanoOfDay() / NANOS_TO_MILLIS);
    }

    public static LocalDateTime toLocalDate(Instant i) {
        return LocalDateTime.ofInstant(i, ZoneOffset.UTC);
    }

    public static LocalTime toTime(Instant i) {
        return toTime(toLocalDate(i));
    }

    public static LocalTime toTime(LocalDateTime ldt) {
        return ldt.toLocalTime();
    }

    public static Instant toDate(LocalDateTime ldt) {
        return ldt.toInstant(ZoneOffset.UTC);
    }

    public static int toIntClamp(double value) {
        if (value <= Integer.MIN_VALUE)
            return Integer.MIN_VALUE;
        if (value >= Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        return (int)value;
    }

    public static int toInt(long value) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
            throw new RuntimeException("Cannot convert to int " + value);
        return (int)value;
    }

    public static double checkRate(double samplingRate) {
        if (samplingRate <= 0 || samplingRate > 1)
            throw new RuntimeException("Invalid sampling rate " + samplingRate);
        return samplingRate;
    }

    /**
     * Convert a long hash code to an int.
     */
    public static int foldHash(long hashCode) {
        return (int)((hashCode & 0xFF) ^ (hashCode >> 32));
    }
}
