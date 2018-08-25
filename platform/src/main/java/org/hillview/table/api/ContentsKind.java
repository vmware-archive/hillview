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

import java.io.Serializable;

/**
 * Describes the kind of data that is in the column,
 */
public enum ContentsKind implements Serializable {
    None,     /* Data kind is unknown */
    Category, /* Categories and strings are the same, but strings cannot be histogrammed */
    String,
    Date,  /* java.time.LocalDateTime values */
    Integer,
    Json,
    Double,
    Duration; /* java.time.Duration values */

    /**
     * True if this kind of information requires a Java Object for storage.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isObject() {
        switch (this) {
            case Category:
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
            case Category:
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
}
