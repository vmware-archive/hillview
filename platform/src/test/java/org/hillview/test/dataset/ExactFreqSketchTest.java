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

import org.hillview.sketches.*;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class ExactFreqSketchTest extends BaseTest {
    // Idea is complaining about the hMap.get calls below,
    // but it also complains if I add explicit casts.
    private void getFrequencies(ITable table, int maxSize) {
        FreqKSketchMG fk = new FreqKSketchMG(table.getSchema(), maxSize);
        FreqKListMG fkList = fk.create(table);
        Assert.assertNotNull(fkList);
        ExactFreqSketch ef = new ExactFreqSketch(table.getSchema(), fkList);
        FreqKListExact exactList = ef.create(table);
        Assert.assertNotNull(exactList);
        int size = 10;
        NextKList nkList = exactList.getTop(table.getSchema());
        for (int i = 1; i < nkList.count.size(); i++) {
            assertTrue(nkList.count.getInt(i - 1) >= nkList.count.getInt(i));
        }
        exactList.filter();
        exactList.getList().forEach(rss ->
                assertTrue(exactList.hMap.getInt(rss) >= fkList.totalRows*fkList.epsilon));
    }

    @Test
    public void EFSTest1() {
        Table t1 = TestTables.testRepTable();
        int maxSize1 = 10;
        getFrequencies(t1, maxSize1);
        SmallTable t2 = TestTables.getHeavyIntTable(2,10000,2,14);
        int maxSize2 = 50;
        getFrequencies(t2, maxSize2);
        SmallTable t3 = TestTables.getHeavyIntTable(2,10000,1.4,20);
        int maxSize3 = 30;
        getFrequencies(t3, maxSize3);
    }
}
