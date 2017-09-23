package org.hillview.test;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;
import org.hillview.table.BaseRowSnapshot;
import org.hillview.table.RowSnapshot;
import org.hillview.table.Schema;
import org.hillview.table.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VirtualRowSnapshotTest {

    public void testSnapshots(ITable data) {
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data);
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        while (i != -1) {
            vrs.setRow(i);
            RowSnapshot rs = new RowSnapshot(data, i);
            int a = vrs.hashCode();
            int b = rs.computeHashCode(data.getSchema());
            assertEquals("Oops!", a, b);
            assertTrue(rs.compareForEquality(vrs, data.getSchema()));
            i = rowIt.getNextRow();
        }
    }

    @Test
    public void VRSTest1() {
        testSnapshots(TestTables.testRepTable());
        testSnapshots(TestTables.getHeavyIntTable( 2, 10000, 1.4, 20));
        testSnapshots(TestTables.getHeavyIntTable( 2, 10000, 2, 14));
    }

    @Test
    public void VRSTest2() {
        ITable data = TestTables.testRepTable();
        Schema schema = data.getSchema();
        HashingStrategy<BaseRowSnapshot> hs = new HashingStrategy<BaseRowSnapshot>() {
            @Override
            public int computeHashCode(BaseRowSnapshot brs) {
                if (brs instanceof VirtualRowSnapshot) {
                    return brs.hashCode();
                } else if (brs instanceof RowSnapshot) {
                    return brs.computeHashCode(schema);
                } else
                    throw new RuntimeException("Uknown type encountered");
            }

            @Override
            public boolean equals(BaseRowSnapshot brs1, BaseRowSnapshot brs2) {
                return brs1.compareForEquality(brs2, schema);
            }
        };
        UnifiedMapWithHashingStrategy<BaseRowSnapshot, Integer> hMap = new
                UnifiedMapWithHashingStrategy<BaseRowSnapshot, Integer>(hs);
        for (int i = 0; i < 2; i++ ) {
            BaseRowSnapshot rs = new RowSnapshot(data, i);
            hMap.put(rs, 0);
        }
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data);
        IRowIterator rowIt = data.getRowIterator();
        vrs.setRow(0);
        if (hMap.containsKey(vrs)) {
            System.out.println("A hit!\n");
            int count = hMap.get(vrs);
            hMap.put(vrs, count + 1);
        } else {
            throw new RuntimeException("Not found");
        }
    }
}
