/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.test.table;

import org.hillview.maps.FilterMap;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.hillview.table.filters.StringFilterDescription;
import org.hillview.table.filters.StringRowFilterDescription;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class StringRowFilterTest extends BaseTest {
    @Test
    public void testFilterSmallTable() {
        // Make a small table
        Table table = TestTables.testRepTable();

        // Make a filter and apply it
        StringRowFilterDescription equalityFilter = new StringRowFilterDescription("Name",
                new StringFilterDescription("Ed"));
        FilterMap filterMap = new FilterMap(equalityFilter);
        ITable result = filterMap.apply(table);
        Assert.assertNotNull(result);

        // Assert number of rows are as expected
        Assert.assertEquals(1, result.getNumOfRows());

        IColumn col = result.getLoadedColumn("Name");
        // Make sure the rows are correct
        IRowIterator it = result.getMembershipSet().getIterator();
        int row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals("Ed", col.getString(row));
            row = it.getNextRow();
        }

        // Same process for Mike.
        equalityFilter = new StringRowFilterDescription("Name",
                new StringFilterDescription("Mike"));
        filterMap = new FilterMap(equalityFilter);
        result = filterMap.apply(table);
        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.getNumOfRows());

        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals("Mike", col.getString(row));
            row = it.getNextRow();
        }
    }

    @Test
    public void testFilterIntegers() {
        // Make a small table
        Table table = TestTables.testRepTable();

        // Make a filter and apply it
        StringRowFilterDescription equalityFilter = new StringRowFilterDescription("Age", new StringFilterDescription("10"));
        FilterMap filterMap = new FilterMap(equalityFilter);
        ITable result = filterMap.apply(table);
        Assert.assertNotNull(result);
        // Assert number of rows are as expected
        Assert.assertEquals(4, result.getNumOfRows());

        // Make a filter and apply it
        equalityFilter = new StringRowFilterDescription("Age", new StringFilterDescription("40"));
        filterMap = new FilterMap(equalityFilter);
        result = filterMap.apply(table);
        Assert.assertNotNull(result);
        // Assert number of rows are as expected
        Assert.assertEquals(2, result.getNumOfRows());
    }

    @Test
    public void testFilterLargeStringTable(){
        // Make a larger ITable
        int size = 500;
        int count = 17;
        String[] possibleNames = {"John", "Robert", "Ed", "Sam", "Ned", "Jaime", "Rickard"};
        String name = "Varys"; // The name we're counting
        ITable table = TestTables.testLargeStringTable(size, possibleNames, count, name);

        // Make the filter map
        StringRowFilterDescription equalityFilter = new StringRowFilterDescription("Name",
                new StringFilterDescription(name));
        FilterMap filterMap = new FilterMap(equalityFilter);

        // Apply the filter map
        ITable result = filterMap.apply(table);
        Assert.assertNotNull(result);
        // Assert that the number of occurrences is correct.
        Assert.assertEquals(count, result.getNumOfRows());
        IColumn col = result.getLoadedColumn("Name");

        // Assert that the correct rows are filtered. (They should all have the same name.)
        IRowIterator it = result.getMembershipSet().getIterator();
        int row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals(name, col.getString(row));
            row = it.getNextRow();
        }
    }
}
