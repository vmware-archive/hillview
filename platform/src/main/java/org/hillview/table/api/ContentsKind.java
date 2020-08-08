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

package org.hillview.table.api;

import javax.annotation.Nullable;

/**
 * Describes the kind of data that is in the column,
 */
public enum ContentsKind {
    None,     /* Data kind is unknown or it's all nulls */
    String,   /* Represented by Strings */
    Date,     /* Internally stored as Double, Objects are Instant */
    Integer,  /* Java 32-bit integers */
    Json,     /* A String that we expect can be parsed as a Json object */
    Double,   /* A Double value */
    Interval, /* A pair of double values that can be converted to double: start, end */
    Time,     /* Internally stored as Double, Objects are LocalTime */
    Duration, /* Internally stored as Double, Objects are Duration */
    LocalDate;/* Internally stored as Double, Objects are LocalDateTime */

    @Nullable
    public Object defaultValue() {
        switch (this) {
            case None:
                return null;
            case String:
            case Json:
                return "";
            case Date:
            case LocalDate:
            case Duration:
            case Double:
            case Time:
                return 0.0;
            case Integer:
                return 0;
            case Interval:
                return org.hillview.table.api.Interval.defaultValue;
            default:
                throw new RuntimeException("Unexpected kind " + this);
        }
    }

    /**
     * True if this kind of information requires a Java Object for storage.
     */
    public boolean isObject() {
        switch (this) {
            case String:
            case Json:
            case None:
            case Interval:
                return true;
            case Date:
            case Duration:
                // We store dates and durations as doubles
            case Integer:
            case Double:
            case Time:
            case LocalDate:
            default:
                return false;
        }
    }

    public boolean isString() {
        switch (this) {
            case String:
            case Json:
                return true;
            case None:
            case Date:
            case Duration:
            case Integer:
            case Double:
            case Interval:
            case Time:
            case LocalDate:
            default:
                return false;
        }
    }

    public boolean isNumeric() {
        switch (this) {
            case String:
            case Json:
            case None:
            case Interval:
                return false;
            case Integer:
            case Double:
            case Date:
            case Time:
            case Duration:
            case LocalDate:
            default:
                return true;
        }
    }
}
