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
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testIntArrayZero() {
        final IntArrayColumn col = new IntArrayColumn(this.desc, this.size);
        for (int i = 0; i < this.size; i++) {
            col.set(i, i);
            if ((i % 5) == 0)
                col.setMissing(i);
        }
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(i, col.getInt(i));
            if ((i % 5) == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }

    /* Test for constructor using data array */
    @Test
    public void testIntArrayOne() {
        final int[] data = new int[this.size];
        for (int i = 0; i < this.size; i++)
            data[i] = i;
        final IntArrayColumn col = new IntArrayColumn(this.desc, data);
        for (int i = 0; i < this.size; i++)
            if ((i % 5) == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(i, col.getInt(i));
            if ((i % 5) == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }

    /* Test for constructor using two arrays: data and missing values*/
    @Test
    public void testIntArrayTwo() {
        final int[] data = new int[this.size];
        final BitSet missing = new BitSet(this.size);
        for (int i = 0; i < this.size; i++) {
            data[i] = i;
            if ((i % 5) == 0)
                missing.set(i);
        }
        final IntArrayColumn col = new IntArrayColumn(this.desc, data, missing);
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            assertEquals(i, col.getInt(i));
            if ((i % 5) == 0)
                assertEquals(true, col.isMissing(i));
            else
                assertEquals(false, col.isMissing(i));
        }
    }
}
