package org.hiero.sketch;

import org.hiero.table.RowSnapshot;
import org.hiero.table.Table;
import org.hiero.table.VirtualRowSnapshot;
import org.hiero.table.api.IRowIterator;
import org.hiero.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class RowSnapShotEqualityTest {
    @Test
    public void RowSnapShotEquailtyTest() {
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
