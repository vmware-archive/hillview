package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.DoubleArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;
import java.util.BitSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Test for DoubleArrayColumn class
 */
class DoubleArrayTest {
    private final int size = 100;
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Double, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testDoubleArrayZero() {
        final DoubleArrayColumn col = new DoubleArrayColumn(this.desc, this.size);
        for (int i = 0; i < this.size; i++) {
            col.set(i, Math.sqrt(i+1));
            if ((i % 5) == 0)
                col.setMissing(i);
        }
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i));
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
        final DoubleArrayColumn col = new DoubleArrayColumn(this.desc, data);
        for (int i = 0; i < this.size; i++)
            if ((i % 5) == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i));
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
        final BitSet missing = new BitSet(this.size);
        for (int i = 0; i < this.size; i++) {
            data[i] = Math.sqrt(i+1);
            if ((i % 5) == 0)
                missing.set(i);
        }
        final DoubleArrayColumn col = new DoubleArrayColumn(this.desc, data, missing);
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i));
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else
                assertFalse(col.isMissing(i));
        }
    }
}
