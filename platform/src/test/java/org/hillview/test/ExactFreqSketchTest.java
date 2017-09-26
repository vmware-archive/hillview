/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.test;

import org.hillview.dataset.api.Pair;
import org.hillview.sketches.ExactFreqSketch;
import org.hillview.sketches.FreqKList;
import org.hillview.sketches.FreqKSketch;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.TestTables;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;


public class ExactFreqSketchTest {
    @SuppressWarnings("SuspiciousMethodCalls")
    // Idea is complaining about the hMap.get calls below,
    // but it also complains if I add explicit casts.
    private void getFrequencies(ITable table, int maxSize) {
        FreqKSketch fk = new FreqKSketch(table.getSchema(), maxSize);
        FreqKList fkList = fk.create(table);
        ExactFreqSketch ef = new ExactFreqSketch(table.getSchema(), fkList);
        FreqKList exactList = ef.create(table);
        int size = 10;
        Pair<List<RowSnapshot>, List<Integer>> pair = exactList.getTop(size);
        for (int i = 1; i < Converters.checkNull(pair.first).size(); i++) {
            Converters.checkNull(pair.second);
            assertTrue(pair.second.get(i - 1) >= pair.second.get(i));
        }
        exactList.filter();
        exactList.getList().forEach(rss ->
                assertTrue(exactList.hMap.get(rss) >= fkList.totalRows/fkList.maxSize ));
    }

    @Test
    public void EFSTest1() {
        Table t1 = TestTables.testRepTable();
        int maxSize1 = 20;
        getFrequencies(t1, maxSize1);
        SmallTable t2 = TestTables.getHeavyIntTable(2,10000,2,14);
        int maxSize2 = 20;
        getFrequencies(t2, maxSize2);
        SmallTable t3 = TestTables.getHeavyIntTable(2,10000,1.4,20);
        int maxSize3 = 30;
        getFrequencies(t3, maxSize3);
    }
}
