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

package org.hiero.sketch;

import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.ITable;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static org.hiero.sketch.TableTest.SplitTable;
import static org.hiero.sketch.TableTest.getIntTable;

public class Issue46Test {
    @Test
    public void createBug() {
        // Creating Int Table
        final SmallTable bigTable = getIntTable(10000, 1);
        // Grabbing the Column
        String colName = bigTable.getSchema().getColumnNames().iterator().next();
        IColumn column  = bigTable.getColumn(colName);
        IMembershipSet memset = bigTable.getMembershipSet();
        IRowIterator iter = memset.getIterator();
        // All seem to work fine
        System.out.println(" printing the double " + column.asDouble(iter.getNextRow(), null));
        System.out.println(" printing the double " + column.asDouble(iter.getNextRow(), null));
        // Splitting the table
        List<SmallTable> tabList = SplitTable(bigTable, 10000);
        // Grabbing the column from  the sub-tables
        ITable subtable = tabList.iterator().next();
        IColumn column1 = subtable.getColumn(colName);
        IMembershipSet memset1 = subtable.getMembershipSet();
        IRowIterator iter1 = memset1.getIterator();
        //Null Exception!!!
        assertNotNull(column1.asDouble(iter1.getNextRow(), null));
        assertNotNull(column1.asDouble(iter1.getNextRow(), null));
    }
}
