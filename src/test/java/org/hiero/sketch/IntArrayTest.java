package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.IntArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;
import java.util.BitSet;
import static junit.framework.TestCase.assertEquals;

/**
 * Test for IntArrayColumn class
 */
class IntArrayTest {
    private final int size = 100;
    private ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testIntArrayZero() {
        IntArrayColumn col = new IntArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, i);
            if (i % 5 == 0)
                col.setMissing(i);
        }
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(i, col.getInt(i));
            if (i % 5 == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }

    /* Test for constructor using data array */
    @Test
    public void testIntArrayOne() {
        int[] data = new int[size];
        for (int i = 0; i < size; i++)
            data[i] = i;
        IntArrayColumn col = new IntArrayColumn(desc, data);
        for (int i = 0; i < size; i++)
            if (i % 5 == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(i, col.getInt(i));
            if (i % 5 == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }

    /* Test for constructor using two arrays: data and missing values*/
    @Test
    public void testIntArrayTwo() {
        int[] data = new int[size];
        BitSet missing = new BitSet(size);
        for (int i = 0; i < size; i++) {
            data[i] = i;
            if (i % 5 == 0)
                missing.set(i);
        }
        IntArrayColumn col = new IntArrayColumn(desc, data, missing);
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(i, col.getInt(i));
            if (i % 5 == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }
}
