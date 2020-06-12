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

package org.hillview.test.dataset;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.ContainsMap;
import org.hillview.maps.FilterMap;
import org.hillview.maps.SetCompareColumnMap;
import org.hillview.table.Table;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.hillview.utils.Utilities;
import org.junit.Assert;
import org.junit.Test;

public class ContainsMapTest extends BaseTest {
    @Test
    public void ContainsTest() {
        Table t = TestTables.testTable();
        RowSnapshot row = new RowSnapshot(t, 0);
        ContainsMap map = new ContainsMap(t.getSchema(), row);
        IDataSet<ITable> ds = new LocalDataSet<ITable>(t);
        IDataSet<ITable> find = ds.blockingMap(map);
        LocalDataSet<ITable> lds = (LocalDataSet<ITable>)find;
        Assert.assertNotNull(lds.data);
        Assert.assertEquals(t.getNumOfRows(), lds.data.getNumOfRows());

        Object[] values = new Object[2];
        values[0] = "Noone";
        values[1] = 10.0;
        row = RowSnapshot.parseJson(t.getSchema(), values, null);
        map = new ContainsMap(t.getSchema(), row);
        ds = new LocalDataSet<ITable>(t);
        find = ds.blockingMap(map);
        lds = (LocalDataSet<ITable>)find;
        Assert.assertNotNull(lds.data);
        Assert.assertEquals(0, lds.data.getNumOfRows());
    }

    @Test
    public void CompareSetsTest() {
        Table t = TestTables.testTable();
        IDataSet<ITable> ds = new LocalDataSet<ITable>(t);
        RangeFilterDescription range = new RangeFilterDescription();
        range.cd = t.getSchema().getDescription("Age");
        range.min = 10;
        range.max = 20;
        IDataSet<ITable> subset0 = ds.blockingMap(new FilterMap(range));
        range.min = 15;
        IDataSet<ITable> subset1 = ds.blockingMap(new FilterMap(range));

        SetCompareColumnMap map = new SetCompareColumnMap("X", Utilities.list("A", "B", "C"));
        IDataSet<ITable> withNewColumn = ds.blockingZipN(Utilities.list(ds, subset0, subset1), map);
        LocalDataSet<ITable> r = (LocalDataSet<ITable>)withNewColumn;
        Assert.assertNotNull(r);
        Assert.assertNotNull(r.data);
        IRowIterator it = r.data.getMembershipSet().getIterator();
        int row = it.getNextRow();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(r.data);
        while (row >= 0) {
            vrs.setRow(row);
            int age = vrs.getInt(range.cd.name);
            String compare = vrs.getString("X");
            if (age < 10 || age > 20)
                Assert.assertEquals("A", compare);
            else if (age < 15)
                Assert.assertEquals("A,B", compare);
            else
                Assert.assertEquals("All", compare);
            row = it.getNextRow();
        }
    }
}
