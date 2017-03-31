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
 *
 */

package org.hiero.sketch.table.api;

import org.hiero.sketch.table.Schema;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;

public interface IRow {
    /**
     * @return The number of fields in the row.
     */
    int rowSize();

    Schema getSchema();

    @Nullable
    Object getObject(final String colName);
    @Nullable
    String getString(String colName);
    Integer getInt(String colName);
    Double getDouble(String colName);
    @Nullable
    LocalDateTime getDate(String colName);
    @Nullable
    Duration getDuration(String colName);

    default ContentsKind getKind(final String colName) {
        return this.getSchema().getKind(colName);
    }
    boolean isMissing(final String colName);
}
