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

package org.hillview.test;

import org.hillview.utils.TestTables;
import org.hillview.table.SmallTable;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class Issue46Test extends BaseTest {
    @Test
    public void createBug() {
        // Creating Int Table
        final SmallTable bigTable = TestTables.getIntTable(10000, 1);
        // Grabbing the Column
        String colName = bigTable.getSchema().getColumnNames()[0];
        IColumn column  = bigTable.getColumn(colName);
        IMembershipSet memSet = bigTable.getMembershipSet();
        IRowIterator iter = memSet.getIterator();
        // All seem to work fine
        System.out.println(" printing the double " + column.asDouble(iter.getNextRow(), null));
        System.out.println(" printing the double " + column.asDouble(iter.getNextRow(), null));
        // Splitting the table
        List<ITable> tabList = TestTables.splitTable(bigTable, 10000);
        // Grabbing the column from  the sub-tables
        ITable subTable = tabList.iterator().next();
        IColumn column1 = subTable.getColumn(colName);
        IMembershipSet memSet1 = subTable.getMembershipSet();
        IRowIterator iter1 = memSet1.getIterator();
        //Null Exception!!!
        Assert.assertNotNull(column1.asDouble(iter1.getNextRow(), null));
        Assert.assertNotNull(column1.asDouble(iter1.getNextRow(), null));
    }
}
