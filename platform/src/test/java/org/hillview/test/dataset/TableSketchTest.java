/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.SummarySketch;
import org.hillview.sketches.TableSummary;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class TableSketchTest extends BaseTest {
    @Test
    public void testTableSummary() {
        ITable bigTable = TestTables.testTable();
        IDataSet<ITable> all = TestTables.makeParallel(bigTable, 5);
        TableSummary summary = all.blockingSketch(new SummarySketch());
        Assert.assertNotNull(summary);
        Assert.assertNotNull(summary.schema);
        Assert.assertEquals(summary.rowCount, bigTable.getNumOfRows());
        Assert.assertEquals(summary.schema, bigTable.getSchema());
    }
}
