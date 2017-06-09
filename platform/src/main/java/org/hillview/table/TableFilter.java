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

package org.hillview.table;
import org.hillview.table.api.ITable;

/**
 * Interface implemented by filters that run over all rows in a table.
 */
public interface TableFilter {
    /**
     * Called before the test is applied to each row index.
     * @param table: Table on which the test operates.
     */
    void setTable(ITable table);

    /**
     * Tests whether a row is selected or not.
     * @param rowIndex Row index in the table.
     */
    boolean test(int rowIndex);
}
