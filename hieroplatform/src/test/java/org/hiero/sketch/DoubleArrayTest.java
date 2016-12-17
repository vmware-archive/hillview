package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.DoubleArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for DoubleArrayColumn class
 */
public class DoubleArrayTest {
    private final int size = 100;
    private static final ColumnDescription desc = new
            ColumnDescription("SQRT", ContentsKind.Double, true);

    /**
     * Generates a double array with every fifth entry missing
     */
    public static DoubleArrayColumn generateDoubleArray(final int size) {
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, Math.sqrt(i + 1));
            if ((i % 5) == 0)
                col.setMissing(i);
        }
        return col;
    }

    /* Test for constructor using length and no arrays*/
    @Test
    public void testDoubleArrayZero() {
        final DoubleArrayColumn col = generateDoubleArray(this.size);
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i), 1e-3);
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else
                assertFalse(col.isMissing(i));
        }
    }

    /* Test for constructor using data array */
    @Test
    public void testDoubleArrayOne() {
        final double[] data = new double[this.size];
        for (int i = 0; i < this.size; i++)
            data[i] = Math.sqrt(i+1);
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, data);
        for (int i = 0; i < this.size; i++)
            if ((i % 5) == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i), 1e-3);
            //System.out.println(col.getDouble(i));
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else
                assertFalse(col.isMissing(i));
        }
    }

    /* Test for constructor using two arrays: data and missing values*/
    @Test
    public void testDoubleArrayTwo() {
        final double[] data = new double[this.size];
        for (int i = 0; i < this.size; i++) {
            data[i] = Math.sqrt(i + 1);
        }
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, data);
        for (int i = 0; i < this.size; i++) {
            if ((i % 5) == 0)
                col.setMissing(i);
        }
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i), 1e-3);
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else
                assertFalse(col.isMissing(i));
        }
    }
}
