package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.IntListColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;

import static org.junit.Assert.*;


public class IntListTest {
    private final int size = 100;
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testIntListColumnZero() {
        final IntListColumn col = new IntListColumn(this.desc);
        for (int i = 0; i < this.size; i++) {
            if (i % 5 != 0)
                col.append(i);
            if ((i % 5) == 0)
                col.appendMissing();
        }
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else {
                assertFalse(col.isMissing(i));
                assertEquals(i, col.getInt(i));
            }
        }
    }
}
