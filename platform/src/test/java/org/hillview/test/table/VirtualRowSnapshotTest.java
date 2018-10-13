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

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import org.hillview.table.Table;
import org.hillview.table.rows.BaseRowSnapshot;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.Schema;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VirtualRowSnapshotTest extends BaseTest {
    @Test
    public void stringTest() {
        final Table table = TestTables.testTable();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(table, table.getSchema());
        Assert.assertEquals("<no such row>", vrs.toString());
        vrs.setRow(0);
        Assert.assertEquals("Mike,20", vrs.toString());
    }

    private void testSnapshots(ITable data) {
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, data.getSchema());
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        while (i != -1) {
            vrs.setRow(i);
            RowSnapshot rs = new RowSnapshot(data, i);
            int a = vrs.hashCode();
            int b = rs.computeHashCode(data.getSchema());
            assertEquals(a, b);
            assertTrue(rs.compareForEquality(vrs, data.getSchema()));
            i = rowIt.getNextRow();
        }
    }

    @Test
    public void testVrsHashing() {
        final int numCols = 1;
        final int size = 1000;
        final double base  = 1.1;
        final int range = 20;
        ITable data = TestTables.getIntTable(size, numCols);
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, data.getSchema());
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        long vHash, hash;
        RowSnapshot rss;
        int num = 10;
        while (i != -1 && i < num) {
            rss = new RowSnapshot(data, i);
            hash = rss.hashCode();
            vrs.setRow(i);
            vHash = vrs.hashCode();
            Assert.assertEquals(hash, vHash);
            i = rowIt.getNextRow();
        }
    }

    @Test
    public void VRSTest1() {
        testSnapshots(TestTables.testRepTable());
        testSnapshots(TestTables.getHeavyIntTable(2, 10000, 1.4, 20));
        testSnapshots(TestTables.getHeavyIntTable(2, 10000, 2, 14));
    }

    @Test
    public void VRSTest2() {
        ITable data = TestTables.testRepTable();
        Schema schema = data.getSchema();
        Hash.Strategy<BaseRowSnapshot> hs = new Hash.Strategy<BaseRowSnapshot>() {
            @Override
            public int hashCode(BaseRowSnapshot brs) {
                if (brs instanceof VirtualRowSnapshot) {
                    return brs.hashCode();
                } else if (brs instanceof RowSnapshot) {
                    return brs.computeHashCode(schema);
                } else
                    throw new RuntimeException("Uknown type encountered");
            }

            @Override
            public boolean equals(BaseRowSnapshot brs1, @Nullable BaseRowSnapshot brs2) {
                // brs2 is null because the hashmap explicitly calls with null
                // even if null cannot be a key.
                if (brs2 == null)
                    return brs1 == null;
                return brs1.compareForEquality(brs2, schema);
            }
        };
        Object2IntMap<BaseRowSnapshot> hMap = new
                Object2IntOpenCustomHashMap<BaseRowSnapshot>(hs);
        for (int i = 0; i < 2; i++ ) {
            BaseRowSnapshot rs = new RowSnapshot(data, i);
            hMap.put(rs, 0);
        }
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, data.getSchema());
        vrs.setRow(0);
        if (hMap.containsKey(vrs)) {
            int count = hMap.getInt(vrs);
            hMap.put(vrs, count + 1);
        } else {
            throw new RuntimeException("Not found");
        }
    }
}
