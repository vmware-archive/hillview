package org.hiero.sketch;

import org.hiero.sketches.CorrMatrix;
import org.hiero.sketches.ExactIPSketch;
import org.hiero.table.SmallTable;
import org.hiero.utils.TestTables;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExactIPTest {

    @Test
    public void ExactIPtest1() {
        int size = 100000;
        int range = 100;
        int numCols = 4;
        SmallTable data = TestTables.getCorrelatedCols(size, numCols, range);
        //System.out.println(data.toLongString(20));
        List<String> cn = new ArrayList<String>();
        for (int i = 0; i < numCols; i++) {
            cn.add("Col" + String.valueOf(i));
        }
        ExactIPSketch ip = new ExactIPSketch(cn);
        CorrMatrix cm = ip.create(data);
        //System.out.println(cm.toString());
        System.out.println(Arrays.toString(cm.getCorrMatrix()[0]));
    }
}
