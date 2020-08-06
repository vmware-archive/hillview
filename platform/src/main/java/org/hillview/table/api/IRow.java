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
import java.util.List;
import java.util.Map;

/**
 * Abstract interface for a row.
 * Our tables are all column-oriented, so the IRow interface should be used sparingly.
 * The Map interface is only partially supported: rows are read-only, so
 * some Map interface methods will throw if invoked.
 */
public interface IRow extends Map<String, Object> {
    /**
     * Sometimes an IRow can be a sentinel value - a row that does not really exist.
     */
    boolean exists();

    /**
     * @return The number of fields in the row.
     */
    int columnCount();

    /**
     * @return The list of columns present in the row.
     */
    List<String> getColumnNames();

    @Nullable
    Object getObject(final String colName);
    @Nullable
    String asString(String colName);
    @Nullable
    String getString(String colName);
    int getInt(String colName);
    double getDouble(String colName);
    double asDouble(String colName);
    /**
     * True if the data is missing in this column.
     * @param colName  Column name.
     */
    boolean isMissing(String colName);
    /**
     * For interval columns - get one of the two endpoints.
     * @param colName  Column name.
     * @param start    If true get the start endpoint, else the end endpoint.
     */
    double getEndpoint(String colName, boolean start);
}
