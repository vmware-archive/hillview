/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.Table;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class RowSnapShotEqualityTest {
    @Test
    public void rowSnapShotEqualityTest() {
        Table t = TestTables.testRepTable();
        int j = 9;
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(t);
        vrs.setRow(j);
        RowSnapshot rs = vrs.materialize();
        IRowIterator rowIt = t.getRowIterator();
        int i = rowIt.getNextRow();
        VirtualRowSnapshot vrs2 = new VirtualRowSnapshot(t);
        while (i != -1) {
            vrs2.setRow(i);
            if (vrs.compareForEquality(vrs2, vrs2.getSchema())) {
                Assert.assertEquals(i, j);
            }
            if (rs.compareForEquality(vrs2, vrs2.getSchema())) {
                Assert.assertEquals(i, j);
            }
            i = rowIt.getNextRow();
        }
    }
}
