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

import org.hillview.dataStructures.IDyadicDecomposition;
import org.hillview.dataStructures.NumericDyadicDecomposition;
import org.hillview.dataStructures.StringDyadicDecomposition;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.results.*;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class DyadicDecompositionTest {
    @Test
    public void testDyadicDecomposition() {
        int leftLeafIdx = 0;
        int rightLeafIdx = 10;

        ArrayList<Pair<Integer, Integer>> ret =
                NumericDyadicDecomposition.dyadicDecomposition(leftLeafIdx, rightLeafIdx);
        Assert.assertNotNull(ret);
        Pair<Integer, Integer> e = ret.get(0);
        Assert.assertNotNull(e);
        Assert.assertNotNull(e.first);
        Assert.assertNotNull(e.second);
        assert(e.first == 0);
        assert(e.second == 8);

        e = ret.get(1);
        Assert.assertNotNull(e.first);
        Assert.assertNotNull(e.second);
        assert(e.first == 8);
        assert(e.second == 2);
    }

    // When the number of buckets is more than the number of leaves,
    // buckets should automatically round down to number of leaves.
    @Test
    public void testTooManyBuckets() {
        final int min = 0;
        final int max = 100;
        final int numBuckets = 10;
        final int granularity = 20;
        IDyadicDecomposition buckDes = new NumericDyadicDecomposition(min, max, numBuckets,
                new DoubleColumnQuantization(granularity, min, max));

        // should create only 100/20 = 5 buckets
        Assert.assertEquals(5, buckDes.getHistogramBuckets().getBucketCount());
    }

    // Test buckets that do not align on leaf boundaries
    @Test
    public void testRaggedBuckets() {
        final int min = 0;
        final int max = 100;
        final int numBuckets = 4; // creates buckets of size 25...
        final int granularity = 10; // but leaves of size 10
        NumericDyadicDecomposition dd = new NumericDyadicDecomposition(min, max, numBuckets,
                new DoubleColumnQuantization(granularity, min, max));
        @SuppressWarnings("unchecked")
        ExplicitHistogramBuckets<Double> buckets =
                (ExplicitHistogramBuckets<Double>)dd.getHistogramBuckets();

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

            Assert.assertEquals(expectedBucket, buckets.indexOf((double)i));
        }

        // Also check computation of bucket size, which is done independently
        for (int i = 0; i < buckets.getBucketCount(); i++) {
            long nLeaves = dd.numLeavesInBucket(i);
            if (i % 2 != 0) {
                Assert.assertEquals(nLeaves, 3);
            } else {
                Assert.assertEquals(nLeaves, 2);
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
        NumericDyadicDecomposition buckDes = new NumericDyadicDecomposition(
                min, max, numBuckets,
                new DoubleColumnQuantization(granularity, min, max));
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
        NumericDyadicDecomposition buckDes = new NumericDyadicDecomposition(
                min, max, numBuckets,
                new DoubleColumnQuantization(granularity, min, max));
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
        NumericDyadicDecomposition dd = new NumericDyadicDecomposition(
                min, max, numBuckets,
                new DoubleColumnQuantization(granularity, min, max));
        @SuppressWarnings("unchecked")
        ExplicitHistogramBuckets<Double> buckets =
                (ExplicitHistogramBuckets<Double>) dd.getHistogramBuckets();

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

            Assert.assertEquals(expectedBucket, buckets.indexOf(i));
        }

        // Also check computation of bucket size, which is done independently
        for (int i = 0; i < buckets.getBucketCount(); i++) {
            long nLeaves = dd.numLeavesInBucket(i);
            if (i % 2 != 0) {
                Assert.assertEquals(nLeaves, 3);
            } else {
                Assert.assertEquals(nLeaves, 2);
            }
        }
    }

    @Test
    public void testBasicStringBuckets() {
        final int numBuckets = 10;
        final double epsilon = 0.01;

        String[] leafLeftBoundaries = new String[10];
        leafLeftBoundaries[0] = "a";
        leafLeftBoundaries[1] = "b";
        leafLeftBoundaries[2] = "c";
        leafLeftBoundaries[3] = "d";
        leafLeftBoundaries[4] = "e";
        leafLeftBoundaries[5] = "f";
        leafLeftBoundaries[6] = "g";
        leafLeftBoundaries[7] = "h";
        leafLeftBoundaries[8] = "i";
        leafLeftBoundaries[9] = "j";
        String max = "k";

        StringDyadicDecomposition buckDes = new StringDyadicDecomposition("a", "k",
                numBuckets, new StringColumnQuantization(leafLeftBoundaries, max));

        @SuppressWarnings("unchecked")
        ExplicitHistogramBuckets<String> buckets =
                (ExplicitHistogramBuckets<String>)buckDes.getHistogramBuckets();
        Assert.assertEquals(buckets.indexOf("defjh"), 3);
    }
}
