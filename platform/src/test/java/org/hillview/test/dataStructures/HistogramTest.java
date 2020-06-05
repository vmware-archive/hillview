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

package org.hillview.test.dataStructures;

import org.hillview.sketches.Histogram2DSketch;
import org.hillview.sketches.results.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.test.BaseTest;
import org.hillview.test.table.DoubleArrayTest;
import org.junit.Assert;
import org.junit.Test;

public class HistogramTest extends BaseTest {
    @Test
    public void testMissing() {
        ColumnDescription c0 = new ColumnDescription("col0", ContentsKind.Double);
        ColumnDescription c1 = new ColumnDescription("col1", ContentsKind.Double);
        IAppendableColumn col0 = BaseListColumn.create(c0);
        IAppendableColumn col1 = BaseListColumn.create(c1);
        for (int i=0; i < 3; i++) {
            col0.append(0.0);
            col0.append(1.0);
            col0.append(2.0);
            col0.appendMissing();
        }

        for (int i = 0; i < 4; i++)
            col1.append(0.0);
        for (int i = 0; i < 4; i++)
            col1.append(1.0);
        for (int i = 0; i < 4; i++)
            col1.appendMissing();

        ITable table = new Table(new IColumn[] { col0, col1 }, null, null);
        DoubleHistogramBuckets buckDes1 = new DoubleHistogramBuckets("col0",0, 2, 3);
        DoubleHistogramBuckets buckDes2 = new DoubleHistogramBuckets("col1", 0, 1, 2);
        Histogram2DSketch sketch = new Histogram2DSketch(buckDes2, buckDes1);
        Groups<Groups<Count>> hm = sketch.create(table);
        Assert.assertNotNull(hm);
        Assert.assertEquals(1, hm.getBucket(0).getBucket(0).count);
        Assert.assertEquals(1, hm.getBucket(0).getBucket(1).count);
        Assert.assertEquals(1, hm.getBucket(1).getBucket(0).count);
        Assert.assertEquals(1, hm.getBucket(1).getBucket(1).count);
        Assert.assertEquals(1, hm.getBucket(2).getBucket(1).count);
        Assert.assertEquals(1, hm.getBucket(2).getBucket(1).count);
        Assert.assertEquals(1, hm.getMissing().getMissing().count);

        Groups<Count> h1 = hm.getMissing();
        Assert.assertEquals(2, h1.size());
        Assert.assertEquals(1, h1.getBucket(0).count);
        Assert.assertEquals(1, h1.getBucket(1).count);

        Assert.assertEquals(1, hm.getBucket(0).getMissing().count);
        Assert.assertEquals(1, hm.getBucket(1).getMissing().count);
        Assert.assertEquals(1, hm.getBucket(2).getMissing().count);
    }
}
