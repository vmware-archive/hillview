package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.ContentsKind;
import org.hiero.sketch.table.IntArrayColumn;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;


public class ColumnTest {
    @Test
    public void testColumn() {
        IntArrayColumn col;
        final int size = 100;

        ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, false);
        col = new IntArrayColumn(desc, size);
        for (int i=0; i < size; i++)
            col.set(i, i);

        assertEquals( col.sizeInRows(), size );
        assertEquals( col.getInt(0), 0 );
        for (int i=0; i < size; i++)
            assertEquals(i, col.getInt(i));
        assertEquals( col.asDouble(0, null), 0.0 );
    }
}
