package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.StringArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;
import java.util.BitSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/*
 * Test for StringArrayColumn class.
*/
class StringArrayTest {

    private final int size = 100;
    private ColumnDescription desc = new ColumnDescription("test", ContentsKind.String, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testStringArrayZero() {
        StringArrayColumn col = new StringArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, String.valueOf(i));
            if (i % 5 == 0)
                col.setMissing(i);
        }
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(String.valueOf(i), col.getString(i));
            if (i % 5 == 0)
                assertTrue(col.isMissing(i));
            else
                assertFalse(col.isMissing(i));
        }
    }

    /* Test for constructor using data array */
    @Test
    public void testStringArrayOne() {
        String[] data = new String[size];
        for (int i = 0; i < size; i++)
            data[i] = String.valueOf(i);
        StringArrayColumn col = new StringArrayColumn(desc, data);
        for (int i = 0; i < size; i++)
            if (i % 5 == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(String.valueOf(i), col.getString(i));
            if (i % 5 == 0)
                assertTrue(col.isMissing(i));
            else
                assertFalse(col.isMissing(i));
        }
    }

    /* Test for constructor using two arrays: data and missing values*/
    @Test
    public void testStringArrayTwo() {
        String[] data = new String[size];
        BitSet missing = new BitSet(size);
        for (int i = 0; i < size; i++) {
            data[i] = String.valueOf(i);
            if (i % 5 == 0)
                missing.set(i);
        }
        StringArrayColumn col = new StringArrayColumn(desc, data, missing);
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(String.valueOf(i), col.getString(i));
            if (i % 5 == 0)
                assertTrue(col.isMissing(i));
            else
                assertFalse(col.isMissing(i));
        }
    }
}
