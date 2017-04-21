package org.hiero.sketch;

import org.hiero.sketches.CorrMatrix;
import org.hiero.sketches.ExactIPSketch;
import org.hiero.sketches.JLProjection;
import org.hiero.sketches.JLSketch;
import org.hiero.table.SmallTable;
import org.hiero.utils.TestTables;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JLSketchTest {

    @Test
    public void JLtest1() {
        int size = 100000;
        int range = 10;
        int numCols = 6;
        SmallTable data = TestTables.getCorrelatedCols(size, numCols, range);
        //System.out.println(data.toLongString(20));
        List<String> cn = new ArrayList<String>();
        for (int i = 0; i < numCols; i++) {
            cn.add("Col" + String.valueOf(i));
        }
        JLSketch jls = new JLSketch(cn, 1000);
        JLProjection jlp = jls.create(data);
        System.out.printf("JL Sketch: " + Arrays.toString(jlp.getCorrMatrix()[0]) +"\n");
        ExactIPSketch ip = new ExactIPSketch(cn);
        CorrMatrix cm = ip.create(data);
        System.out.printf("IP Sketch: " + Arrays.toString(cm.getCorrMatrix()[0]) + "\n");
    }

    @Test
    public void JLtest2() {
        int size = 10000;
        int range = 10;
        int numCols = 4;
        SmallTable leftTable = TestTables.getCorrelatedCols(size, numCols,  range);
        SmallTable rightTable = TestTables.getCorrelatedCols(size, numCols, range);
        List<String> cn = new ArrayList<String>();
        for (int i = 0; i < numCols; i++) {
            cn.add("Col" + String.valueOf(i));
        }
        JLSketch jls = new JLSketch(cn, 1000);
        JLProjection jlp = jls.add(jls.create(leftTable), jls.create(rightTable));
        System.out.println(Arrays.toString(jlp.getCorrMatrix()[0]));
        ExactIPSketch ip = new ExactIPSketch(cn);
        CorrMatrix cm = ip.add(ip.create(leftTable), ip.create(rightTable));
        System.out.printf("IP Sketch: " + Arrays.toString(cm.getCorrMatrix()[0]) + "\n");
    }
}