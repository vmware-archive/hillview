package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.StringListColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;

import static junit.framework.TestCase.*;

/*
 * Test for StringArrayColumn class.
*/
public class StringListTest {
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.String, true);

    @Test
    public void testStringArrayZero() {
        final StringListColumn col = new StringListColumn(this.desc);
        final int size = 1000;
        for (int i = 0; i < size; i++) {
            if ((i % 5) == 0) {
                col.appendMissing();
            } else {
                col.append(String.valueOf(i));
            }
        }
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            if ((i % 5) == 0) {
                assertTrue(col.isMissing(i));
            } else {
                assertFalse(col.isMissing(i));
                assertEquals(String.valueOf(i), col.getString(i));
            }
        }
    }
}
