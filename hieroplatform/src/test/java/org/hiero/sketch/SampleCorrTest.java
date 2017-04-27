package org.hiero.sketch;

import org.hiero.sketches.CorrMatrix;
import org.hiero.sketches.SampleCorrelationSketch;
import org.hiero.table.SmallTable;
import org.hiero.utils.TestTables;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleCorrTest {

    @Test
    public void SampleCorrTest1() {
        int size = 40000;
        int range = 50000;
        int numCols = 5;
        SmallTable data = TestTables.getCorrelatedCols(size, numCols, range);
        //System.out.println(data.toLongString(20));
        List<String> cn = new ArrayList<String>();
        for (int i = 0; i < numCols; i++) {
            cn.add("Col" + String.valueOf(i));
        }
        for (int i = 0; i <= 10; i++) {
            SampleCorrelationSketch ip = new SampleCorrelationSketch(cn, Math.pow(0.5, i));
            CorrMatrix cm = ip.create(data);
            System.out.printf("Sampling rate %f: ", Math.pow(0.5, i));
            System.out.println(Arrays.toString(cm.getCorrMatrix()[0]));
            System.out.printf("\n");
        }
    }
}
