/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;

import org.hillview.sketches.DyadicDoubleHistogramBuckets;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DyadicBucketsTest {
    // Generate a column s.t. value i for i in [minVal, maxVal) is repeated nPerValue times.
    private static DoubleArrayColumn generateLinearColumn(final int minVal, final int maxVal, final int nPerValue) {
        final ColumnDescription desc = new
                  ColumnDescription("Linear", ContentsKind.Double);
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, (maxVal - minVal) * nPerValue);
        for (int i = minVal; i < maxVal; i++) {
            for (int j = 0; j < nPerValue; j++) {
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

        for (int i = 0; i < numValues; i++) {
            for (int j = 0; j < nPerValue; j++) {
                assertEquals(col.getDouble(i*nPerValue+j), i, 1e-3);
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
        final double epsilon = 0.01;
        DyadicDoubleHistogramBuckets buckDes = new DyadicDoubleHistogramBuckets(min, max, numBuckets,
                new DoubleColumnPrivacyMetadata(epsilon, granularity, min, max));

        // should create only 100/20 = 5 buckets
        assert(buckDes.getBucketCount() == 5);
    }

    // Test buckets that do not align on leaf boundaries
    @Test
    public void testRaggedBuckets() {
        final int min = 0;
        final int max = 100;
        final int numBuckets = 4; // creates buckets of size 25...
        final int granularity = 10; // but leaves of size 10
        final double epsilon = 0.01;
        DyadicDoubleHistogramBuckets buckDes = new DyadicDoubleHistogramBuckets(min, max, numBuckets,
                new DoubleColumnPrivacyMetadata(epsilon, granularity, min, max));

        // Check that values fall in correct buckets based on leaves
        int expectedBucket;
        for (int i = 0; i < max; i++) {
            // Explicitly compute buckets for testing.
            // Bucket should cover all leaves whose left boundary falls in bucket.
            if (i < 20) {
                expectedBucket = 0;
            } else if (i < 50) {
                expectedBucket = 1;
            } else if (i < 70) {
                expectedBucket = 2;
            } else {
                expectedBucket = 3;
            }

            assertEquals(expectedBucket, buckDes.indexOf(i));
        }

        // Also check computation of bucket size, which is done independently
        for (int i = 0; i < buckDes.getBucketCount(); i++) {
            long nLeaves = buckDes.numLeavesInBucket(i);
            if (i % 2 != 0) {
                assertEquals(nLeaves, 3);
            } else {
                assertEquals(nLeaves, 2);
            }
        }
    }

    // Test leaf assignment to buckets
    @Test
    public void testLeafAssignment() {
        final double min = 0;
        final double max = 0.1;
        final int numBuckets = 4; // creates buckets of size 0.025...
        final double granularity = 0.01; // but leaves of size 0.01
        final double epsilon = 0.01;
        DyadicDoubleHistogramBuckets buckDes = new DyadicDoubleHistogramBuckets(min, max, numBuckets,
                new DoubleColumnPrivacyMetadata(epsilon, granularity, min, max));
        // TODO
        /*
        for (int i = 0; i < buckDes.getNumOfBuckets(); i++) {
            System.out.println("> " + i);
            System.out.println(buckDes.bucketLeafIdx(i));
        }
         */
    }

    // Test negative range
    @Test
    public void testNegativeRange() {
        final double min = -100;
        final double max = 100;
        final int numBuckets = 10;
        final double granularity = 25;
        final double epsilon = 0.01;
        DyadicDoubleHistogramBuckets buckDes = new DyadicDoubleHistogramBuckets(min, max, numBuckets,
                new DoubleColumnPrivacyMetadata(epsilon, granularity, min, max));
        // TODO
        /*
        for (int i = 0; i < buckDes.getNumOfBuckets(); i++) {
            System.out.println("> " + i);
            System.out.println(buckDes.bucketLeafIdx(i));
        }
         */
    }

    // Test granularity < 1
    @Test
    public void testSmallGranularity() {
        final double min = 0;
        final double max = 0.1;
        final int numBuckets = 4; // creates buckets of size 0.025...
        final double granularity = 0.01; // but leaves of size 0.01
        final double epsilon = 0.01;
        DyadicDoubleHistogramBuckets buckDes = new DyadicDoubleHistogramBuckets(min, max, numBuckets,
                new DoubleColumnPrivacyMetadata(epsilon, granularity, min, max));

        // Check that values fall in correct buckets based on leaves
        int expectedBucket;
        for (double i = 0; i < max; i+= 0.01) {
            // Explicitly compute buckets for testing.
            // Boundaries are based on result of floating-point arithmetic.
            if (i < 0.02) {
                expectedBucket = 0;
            } else if (i < 0.05) {
                expectedBucket = 1;
            } else if (i < 0.07) {
                expectedBucket = 2;
            } else {
                expectedBucket = 3;
            }

            assertEquals(expectedBucket, buckDes.indexOf(i));
        }

        // Also check computation of bucket size, which is done independently
        for (int i = 0; i < buckDes.getBucketCount(); i++) {
            long nLeaves = buckDes.numLeavesInBucket(i);
            if (i % 2 != 0) {
                assertEquals(nLeaves, 3);
            } else {
                assertEquals(nLeaves, 2);
            }
        }
    }
}
