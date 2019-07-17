package org.hillview.test.dataStructures;

import org.hillview.sketches.DyadicHistogramBuckets;
import org.hillview.sketches.Histogram;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.test.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrivateHistogramTest extends BaseTest {
    // Generate a column s.t. value i for i in [minVal, maxVal) is repeated nPerValue times.
    private static DoubleArrayColumn generateLinearColumn(final int minVal, final int maxVal, final int nPerValue) {
        final ColumnDescription desc = new
                  ColumnDescription("Linear", ContentsKind.Double);
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, (maxVal - minVal) * nPerValue);
        for ( int i = minVal; i < maxVal; i++ ) {
            for ( int j = 0; j < nPerValue; j++ ) {
                col.set(i*nPerValue + j, (double)i);
            }
        }
        return col;
    }

    // Just make sure the linear column generator works
    @Test
    public void testLinearColumnContents() {
        final int numValues = 13;
        final int nPerValue = 7;

        DoubleArrayColumn col = generateLinearColumn(0, numValues, nPerValue);

        for ( int i = 0; i < numValues; i++ ) {
            for ( int j = 0; j < nPerValue; j++ ) {
                assertEquals(col.getDouble(i*nPerValue+j), (double)i, 1e-3);
            }
        }
    }

    // When the number of buckets is more than the number of leaves,
    // buckets should automatically round down to number of leaves.
    @Test
    public void testTooManyBuckets() {
        final int min = 0;
        final int max = 100;
        final int numBuckets = 10;
        final int granularity = 20;
        DyadicHistogramBuckets buckDes = new DyadicHistogramBuckets(min, max, numBuckets, granularity);

        // should create only 100/20 = 5 buckets
        assert(buckDes.getNumOfBuckets() == 5);
    }

    // Test buckets that do not align on leaf boundaries
    @Test
    public void testRaggedBuckets() {
        final int min = 0;
        final int max = 100;
        final int numBuckets = 4; // creates buckets of size 25...
        final int granularity = 10; // but leaves of size 10

        DyadicHistogramBuckets buckDes = new DyadicHistogramBuckets(min, max, numBuckets, granularity);

        // Check that values fall in correct buckets based on leaves
        int expectedBucket;
        for ( int i = 0; i < max; i++ ) {
            // Explicitly compute buckets for testing.
            // Bucket should cover all leaves whose left boundary falls in bucket.
            if ( i < 30 ) {
                expectedBucket = 0;
            } else if ( i < 50 ) {
                expectedBucket = 1;
            } else if ( i < 80 ) {
                expectedBucket = 2;
            } else {
                expectedBucket = 3;
            }

            assertEquals(buckDes.indexOf((double)i), expectedBucket);
        }

        // Also check computation of bucket size, which is done independently
        for ( int i = 0; i < buckDes.getNumOfBuckets(); i++ ) {
            int nLeaves = buckDes.numLeavesInBucket(i);
            if ( i % 2 == 0 ) {
                assertEquals(nLeaves, 3);
            } else {
                assertEquals(nLeaves, 2);
            }
        }
    }

    // Test requested min/max that do not align on leaf boundaries
    @Test
    public void testRaggedMinMax() {
        final int min = 1;
        final int max = 99;
        final int numBuckets = 10;
        final int granularity = 20;
        DyadicHistogramBuckets buckDes = new DyadicHistogramBuckets(min, max, numBuckets, granularity);

        // min should round down to 0, max up to 100
        assertEquals(buckDes.getMin(), 0, 1e-3);
        assertEquals(buckDes.getMax(), 100, 1e-3);
    }

    // Test computation of noise scale when buckets do not align on leaf boundaries
    @Test
    public void testRaggedNoiseMultiplier() {
        final int min = 0;
        final int max = 100;
        final int numBuckets = 4; // creates buckets of size 25...
        final int granularity = 10; // but leaves of size 10
        DyadicHistogramBuckets buckDes = new DyadicHistogramBuckets(min, max, numBuckets, granularity);
        Histogram hist = new Histogram(buckDes);

        DoubleArrayColumn col = generateLinearColumn(min, max, 1);
        FullMembershipSet fMap = new FullMembershipSet(col.sizeInRows());
        hist.create(col, fMap, 1.0, 0, false);

        // manually compute expected multiplier
        for ( int i = 0; i < hist.getNumOfBuckets(); i++ ) {
            if ( i % 2 == 0 ) {
                assertEquals(hist.noiseMultiplier(i), 2);
            } else {
                assertEquals(hist.noiseMultiplier(i), 1);
            }
        }

        // make sure we can add noise
        double T = 10.0;
        double eps = 0.05;
        hist.addDyadicLaplaceNoise(T / eps);
    }
}
