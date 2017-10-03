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

import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.*;
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.utils.JsonList;
import org.hillview.utils.TestTables;
import org.hillview.table.SemiExplicitConverter;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

public class DistinctStringSketchTest extends BaseTest {
    private SemiExplicitConverter getStringConverter(DistinctStrings ds) {
        SemiExplicitConverter converter = new SemiExplicitConverter();
        int i = 0;
        for (String item : ds.getStrings()) {
            converter.set(item, i);
            i++;
        }
        return converter;
    }

    @Test
    public void DistinctSketchTest() {
        final int tableSize = 1000;
        final Table myTable = TestUtil.createTable(tableSize);
        String[] columns = new String[] { "Name" };
        final DistinctStringsSketch mySketch = new DistinctStringsSketch(20, columns);
        JsonList<DistinctStrings> result = mySketch.create(myTable);
        Assert.assertEquals(result.size(), 1);
        int size = result.get(0).size();
        Assert.assertTrue(size <= 10);
        SemiExplicitConverter converter = getStringConverter(result.get(0));
        BucketsDescriptionEqSize desc = new BucketsDescriptionEqSize(1, size + 1, size);
        HistogramSketch histSketch = new HistogramSketch(
                desc, new ColumnNameAndConverter("Name", converter));
        Histogram hist = histSketch.create(myTable);
    }

    @Test
    public void DistinctSketchTest2() {
        final int tableSize = 1000;
        final SmallTable myTable = TestUtil.createSmallTable(tableSize);
        final ParallelDataSet<ITable> all = TestTables.makeParallel(myTable, tableSize/10);
        String[] columns = new String[] { "Name" };
        final JsonList<DistinctStrings> ds = all.blockingSketch(
                new DistinctStringsSketch(tableSize, columns));
        Assert.assertEquals(ds.size(), 1);
        SemiExplicitConverter converter = getStringConverter(ds.get(0));
        BucketsDescriptionEqSize desc = new BucketsDescriptionEqSize(
                -1, ds.get(0).size(), ds.get(0).size() + 1);
        Histogram hist = all.blockingSketch(
                new HistogramSketch(desc, new ColumnNameAndConverter("Name", converter)));
    }
}
