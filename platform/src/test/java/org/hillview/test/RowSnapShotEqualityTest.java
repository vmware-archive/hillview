package org.hillview.test;

import org.hillview.table.RowSnapshot;
import org.hillview.table.Table;
import org.hillview.table.VirtualRowSnapshot;
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
