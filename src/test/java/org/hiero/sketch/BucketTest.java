package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.Bucket;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import org.junit.Test;
import java.util.Comparator;

public class BucketTest {
    @Test
    public void testBucket() throws Exception {
        final Bucket<Integer> myBucket = new Bucket<Integer>(10, true, 20, true, Comparator.<Integer>naturalOrder());
        assertTrue(myBucket.inBucket(15));
        assertTrue(myBucket.inBucket(10));
        assertTrue(myBucket.inBucket(20));
        assertFalse(myBucket.inBucket(21));
        assertFalse(myBucket.inBucket(9));
        final Bucket<Integer> myBucket1 = new Bucket<Integer>(10, true, 10, true, Comparator.<Integer>naturalOrder());
        assertTrue(myBucket1.inBucket(10));
        assertFalse(myBucket1.inBucket(21));
        assertFalse(myBucket1.inBucket(9));
    }
}
