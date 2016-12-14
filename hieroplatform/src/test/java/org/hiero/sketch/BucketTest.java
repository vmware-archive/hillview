package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.Bucket1D;
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
        assertTrue(myBucket.getCount() == 100);
        assertTrue(myBucket1.getCount() == 100);
        assertTrue(myBucket1.getMinValue() == 0.5);
        assertTrue(myBucket1.getMaxValue() == 99.5);
        final Bucket1D myBucket2 = myBucket.union(myBucket1);
        assertTrue(myBucket2.getCount() == 200);
        assertTrue(myBucket2.getMinValue() == 0);
        assertTrue(myBucket2.getMaxValue() == 99.5);
        assertTrue(myBucket2.getMinObject().equals("0"));
        assertTrue(myBucket2.getMaxObject().equals("99.5"));
    }
}
