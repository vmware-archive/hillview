package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.IntArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for IntArrayColumn class
 */
public class IntArrayTest {
    static private final ColumnDescription desc = new
            ColumnDescription("Identity", ContentsKind.Int, true);

    public static IntArrayColumn generateIntArray(final int size) {
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, i);
            if ((i % 5) == 0)
                col.setMissing(i);
        }
        return col;
    }

    /* Test for constructor using length and no arrays*/
    @Test
    public void testIntArrayZero() {
        final int size = 100;
        final IntArrayColumn col = generateIntArray(size);
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else {
                assertFalse(col.isMissing(i));
                assertEquals(i, col.getInt(i));
            }
        }
    }

    /* Test for constructor using data array */
    @Test
    public void testIntArrayOne() {
        final int size = 100;

        final int[] data = new int[size];
        for (int i = 0; i < size; i++)
            data[i] = i;
        final IntArrayColumn col = new IntArrayColumn(desc, data);
        for (int i = 0; i < size; i++)
            if ((i % 5) == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(i, col.getInt(i));
            if ((i % 5) == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }
}
