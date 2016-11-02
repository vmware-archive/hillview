package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.DoubleListColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;
import static java.lang.Math.sqrt;
import static org.junit.Assert.*;

public class DoubleListTest {
    private final int size = 100;
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Double, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testIntListColumnZero() {
        final DoubleListColumn col = new DoubleListColumn(this.desc);
        for (int i = 0; i < this.size; i++) {
            if (i % 5 != 0)
                col.append(sqrt(i+1));
            if ((i % 5) == 0)
                col.appendMissing();
        }
        assertEquals(col.sizeInRows(), this.size);
        for (int i = 0; i < this.size; i++) {
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else {
                assertFalse(col.isMissing(i));
                assertEquals(sqrt(i+1), col.getDouble(i), 1.0E-02);
            }
        }
    }
}
