package org.hiero.sketch;

import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.VirtualRowSnapshot;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class RowSnapShotEqualityTest {

    @Test
    public void RowSnapShotEquailtyTest() {
        Table t = TestTables.testRepTable();
        int j = 9;
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(t, j);
        RowSnapshot rs = vrs.materialize();
        IRowIterator rowIt = t.getRowIterator();
        int i = rowIt.getNextRow();
        while (i != -1) {
            VirtualRowSnapshot vrs2 = new VirtualRowSnapshot(t, i);
            if (vrs.equals(vrs2)) {
                Assert.assertEquals(i, j);
            }
            if (rs.equals(vrs2)) {
                Assert.assertEquals(i, j);
            }
            i = rowIt.getNextRow();
        }
    }
}
