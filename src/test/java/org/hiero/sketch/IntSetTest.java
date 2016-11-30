package org.hiero.sketch;

import org.hiero.sketch.table.IntSet;
import org.junit.Test;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;


public class IntSetTest {

    @Test
    public void TestIntSet() {
        final int size = 1000000;
        final IntSet IS = new IntSet(50,0.75F);
        for (int i = 0; i < size; i++ )
           IS.add(i);
        for (int i = 0; i < size; i++ )
            IS.add(i);
        assertEquals(size, IS.size());
        assertTrue(IS.contains(7));
        assertFalse(IS.contains(-200));
        final IntSet IS1 = IS.copy();
        for (int i = 0; i < size; i++ )
            IS1.add(i);
        assertEquals(size, IS1.size());
        assertTrue(IS1.contains(7));
        assertFalse(IS1.contains(-200));
    }
}
