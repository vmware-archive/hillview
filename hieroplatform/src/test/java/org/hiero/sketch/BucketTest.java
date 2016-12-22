package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.Bucket1D;
import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;
import org.junit.Test;

public class BucketTest {
    @Test
    public void testBucket() throws Exception {
        final Bucket1D myBucket = new Bucket1D();
        final Bucket1D myBucket1 = new Bucket1D();
        assertTrue(myBucket.isEmpty());
        for (int i = 0; i < 100; i++) {
            myBucket.add((double) i, Integer.toString(i));
            myBucket1.add( 99.5 - i , Double.toString(99.5 - i));
        }
        assertEquals(myBucket.getCount(), 100);
        assertEquals(myBucket1.getCount(), 100);
        assertEquals(myBucket1.getMinValue(), 0.5);
        assertEquals(myBucket1.getMaxValue(), 99.5);
        final Bucket1D myBucket2 = myBucket.union(myBucket1);
        assertEquals(myBucket2.getCount(), 200);
        assertEquals(myBucket2.getMinValue(), 0.0);
        assertEquals(myBucket2.getMaxValue(), 99.5);
        assertEquals(myBucket2.getMinObject(), "0");
        assertEquals(myBucket2.getMaxObject(), "99.5");
    }
}
