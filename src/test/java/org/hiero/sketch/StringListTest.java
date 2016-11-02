
package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.StringListColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;

import static junit.framework.TestCase.*;

/*
 * Test for StringArrayColumn class.
*/
class StringListTest {

    private final int size = 1000;
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.String, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testStringArrayZero() {
        final StringListColumn col = new StringListColumn(this.desc);
        for (int i = 0; i < this.size; i++) {
            col.append(String.valueOf(i));
            if ((i % 5) == 0) {
                col.appendMissing();
            }
        }
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            if ((i % 5) == 0) {
                assertTrue(col.isMissing(i));
            } else {
                assertFalse(col.isMissing(i));
                assertEquals(String.valueOf(i), col.getString(i));
            }
        }
    }
}
