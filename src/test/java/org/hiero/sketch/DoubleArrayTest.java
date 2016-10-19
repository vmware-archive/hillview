package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.DoubleArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;

import java.util.BitSet;

import static junit.framework.TestCase.assertEquals;

/**
 * Created by parik on 10/17/16.
 */
public class DoubleArrayTest {

    final int size = 100;
    ColumnDescription desc = new ColumnDescription("test", ContentsKind.Double, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testDoubleArrayZero() {
        DoubleArrayColumn col = new DoubleArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, Math.sqrt(i+1));
            if (i % 5 == 0)
                col.setMissing(i);
        }
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i));
            if (i % 5 == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }

    /* Test for constructor using data array */
    @Test
    public void testDoubleArrayOne() {
        double[] data = new double[size];
        for (int i = 0; i < size; i++)
            data[i] = Math.sqrt(i+1);
        DoubleArrayColumn col = new DoubleArrayColumn(desc, data);
        for (int i = 0; i < size; i++)
            if (i % 5 == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i));
            //System.out.println(col.getDouble(i));
            if (i % 5 == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }

    /* Test for constructor using two arrays: data and missing values*/
    @Test
    public void testDoubleArrayTwo() {
        double[] data = new double[size];
        BitSet missing = new BitSet(size);
        for (int i = 0; i < size; i++) {
            data[i] = Math.sqrt(i+1);
            if (i % 5 == 0)
                missing.set(i);
        }
        DoubleArrayColumn col = new DoubleArrayColumn(desc, data, missing);
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i));
            if (i % 5 == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }
}
