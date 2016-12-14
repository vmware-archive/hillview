package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.BucketsDescription1D;
import org.hiero.sketch.spreadsheet.BucketsDescriptionEqSize;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class BucketsDescription1DTest {
    @Test
    public void testEqSize() throws Exception {
        BucketsDescriptionEqSize bdEqSize = new BucketsDescriptionEqSize(0.5, 100.5, 100);
        assertTrue(bdEqSize.getNumOfBuckets() == 100);
        assertTrue(bdEqSize.indexOf(0.5) == 0);
        assertTrue(bdEqSize.indexOf(0.6) == 0);
        assertTrue(bdEqSize.indexOf(100.5) == 99);
        assertTrue(bdEqSize.indexOf(100.4) == 99);
        assertTrue(bdEqSize.indexOf(70.5) == 70);
        assertTrue(bdEqSize.indexOf(30.6) == 30);
        assertTrue(bdEqSize.getLeftBoundary(23) == 23.5);
        assertTrue(bdEqSize.getRightBoundary(23) == 24.5);
        assertTrue(bdEqSize.getRightBoundary(99) == 100.5);

        BucketsDescriptionEqSize bdEqSize1 = new BucketsDescriptionEqSize(0.5, 100.5, 99);
        assertFalse(bdEqSize.equals(bdEqSize1));
        BucketsDescriptionEqSize bdEqSize2 = new BucketsDescriptionEqSize(0.5, 100.5, 99);
        assertTrue(bdEqSize2.equals(bdEqSize1));
        double[] boundaries = new double[101];
        for (int i = 0; i < 101; i++)
            boundaries[i] = 0.5 + i;
        assertTrue(Arrays.equals(boundaries, bdEqSize.getBoundaries()));
    }

    @Test
    public void testGeneric1D() throws Exception {
        double[] boundaries = new double[101];
        for (int i = 0; i < 101; i++)
            boundaries[i] = i + 0.5;
        BucketsDescription1D bdEq = new BucketsDescription1D(boundaries);
        assertTrue(bdEq.getNumOfBuckets() == 100);
        assertTrue(bdEq.indexOf(0.5) == 0);
        assertTrue(bdEq.indexOf(0.6) == 0);
        assertTrue(bdEq.indexOf(100.5) == 99);
        assertTrue(bdEq.indexOf(100.4) == 99);
        assertTrue(bdEq.indexOf(70.5) == 70);
        assertTrue(bdEq.indexOf(30.6) == 30);
        assertTrue(bdEq.getLeftBoundary(23) == 23.5);
        assertTrue(bdEq.getRightBoundary(23) == 24.5);
        assertTrue(bdEq.getRightBoundary(99) == 100.5);
        double[] boundaries1 = new double[101];
        for (int i = 0; i < 101; i++)
            boundaries1[i] = i + 0.4;
        BucketsDescription1D bdEq1 = new BucketsDescription1D(boundaries1);
        BucketsDescription1D bdEq2 = new BucketsDescription1D(bdEq.getBoundaries());
        assertFalse(bdEq.equals(bdEq1));
        assertTrue(bdEq.equals(bdEq2));
    }
}
