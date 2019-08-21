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

import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Describes the kind of data that is in the column,
 */
public enum ContentsKind implements Serializable {
    None,     /* Data kind is unknown */
    String,
    Date,  /* java.time.LocalDateTime values */
    Integer,
    Json,
    Double,
    Duration; /* java.time.Duration values */

    /**
     * The minimum value representable by this kind.
     */
    @Nullable
    public Object minimumValue() {
        switch (this) {
            case None:
                return null;
            case String:
            case Json:
                return "";
            case Date:
                return Converters.toDate(-java.lang.Double.MAX_VALUE);
            case Integer:
                return java.lang.Integer.MIN_VALUE;
            case Double:
                return -java.lang.Double.MAX_VALUE;
            case Duration:
                return Converters.toDuration(-java.lang.Double.MAX_VALUE);
            default:
                throw new RuntimeException("Unexpected kind " + this);
        }
    }

    /**
     * True if this kind of information requires a Java Object for storage.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isObject() {
        switch (this) {
            case String:
            case Json:
            case None:
                return true;
            case Date:
            case Duration:
                // We store dates and durations as doubles
            case Integer:
            case Double:
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
            default:
                return false;
        }
    }

    public boolean isNumeric() {
        switch (this) {
            case String:
            case Json:
            case None:
            case Date:
                return false;
            case Duration:
            case Integer:
            case Double:
            default:
                return true;
        }
    }
}
