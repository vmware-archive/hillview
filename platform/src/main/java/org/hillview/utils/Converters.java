/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.utils;

import javax.annotation.Nullable;
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

    public static double toDouble(final LocalDateTime d) {
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
    public static LocalDateTime toDate(final double d) {
        Duration span = toDuration(d);
        return baseTime.plus(span);
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
}
