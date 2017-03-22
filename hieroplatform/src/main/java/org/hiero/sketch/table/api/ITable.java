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
import org.hiero.sketch.table.SmallTable;

/**
 * An ITable object has a schema, a set of columns, and a MembershipSet.
 * All columns have the same size.
 */
public interface ITable {
    Schema getSchema();

    IRowIterator getRowIterator();

    /**
     * Describes the set of rows that are really present in the table.
     */
    IMembershipSet getMembershipSet();

    int getNumOfRows();

    IColumn getColumn(String colName);

    /**
     * Creates a small table by keeping only the rows in the IRowOrder and
     * the columns in the subSchema.
     * @param subSchema Indicates columns to keep.
     * @param rowOrder  Indicates rows to keep.
     */
    SmallTable compress(ISubSchema subSchema, IRowOrder rowOrder);

    SmallTable compress(IRowOrder rowOrder);

    /**
     * Creates a new table which is identical with this one except the
     * provided membership set.
     * @param set: Membership set of the resulting table.
     */
    ITable filter(IMembershipSet set);
}
