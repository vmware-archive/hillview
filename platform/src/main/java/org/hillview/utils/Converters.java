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
    private static final Instant baseTime = LocalDateTime.of(
            LocalDate.of(1970, 1, 1),
            LocalTime.of(0, 0)).toInstant(ZoneOffset.UTC);

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

    public static Duration toDuration(final double d) {
        return Duration.ofMillis((long)d);
    }

    /**
     * Casts a Nullable pointer to a NonNullable one.  Throws if pointer is null.
     * @param data  Nullable pointer.
     * @param <T>   Type of nullable pointer.
     * @return      The same pointer, but never null.
     */
    public static <T> T checkNull(@Nullable T data) {
        if (data == null)
            throw new NullPointerException("Converters.checkNull() failed");
        return data;
    }

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS")
            .withLocale(Locale.US)
            .withZone(ZoneId.systemDefault());

    /**
     * Convert a date to a string.  Must be the same algorithm as the
     * one used in JavaScript to convert dates.
     */
    public static String toString(@Nullable Instant d) {
        if (d == null)
            return "missing";
        String s = formatter.format(d);
        if (s.endsWith(".000"))
            s = s.substring(0, s.length() - 4);
        if (s.endsWith(" 00:00:00"))
            return s.substring(0, s.length() - 9);
        if (s.endsWith(":00"))
            return s.substring(0, s.length() - 3);
        return s;
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
}
