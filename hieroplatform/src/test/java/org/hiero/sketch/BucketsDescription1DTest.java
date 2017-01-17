package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.BucketsDescription1D;
import org.hiero.sketch.spreadsheet.BucketsDescriptionEqSize;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BucketsDescription1DTest {
    @Test
    public void testEqSize() throws Exception {
        BucketsDescriptionEqSize bdEqSize = new BucketsDescriptionEqSize(0.5, 100.5, 100);
        assertEquals(bdEqSize.getNumOfBuckets(), 100);
        assertEquals(bdEqSize.indexOf(0.5), 0);
        assertEquals(bdEqSize.indexOf(0.6), 0);
        assertEquals(bdEqSize.indexOf(100.5), 99);
        assertEquals(bdEqSize.indexOf(100.4), 99);
        assertEquals(bdEqSize.indexOf(70.5), 70);
        assertEquals(bdEqSize.indexOf(30.6), 30);
        assertEquals(bdEqSize.getLeftBoundary(23), 23.5, .1);
        assertEquals(bdEqSize.getRightBoundary(23), 24.5, .1);
        assertEquals(bdEqSize.getRightBoundary(99), 100.5, .1);
        BucketsDescriptionEqSize bdEqSize1 = new BucketsDescriptionEqSize(0.5, 100.5, 99);
        assertFalse(bdEqSize.equals(bdEqSize1));
        BucketsDescriptionEqSize bdEqSize2 = new BucketsDescriptionEqSize(0.5, 100.5, 99);
        assertTrue(bdEqSize2.equals(bdEqSize1));
    }

    @Test
    public void testGeneric1D() throws Exception {
        double[] boundaries = new double[101];
        for (int i = 0; i < 101; i++)
            boundaries[i] = i + 0.5;
        BucketsDescription1D bdEq = new BucketsDescription1D(boundaries);
        assertEquals(bdEq.getNumOfBuckets(), 100);
        assertEquals(bdEq.indexOf(0.5), 0);
        assertEquals(bdEq.indexOf(0.6), 0);
        assertEquals(bdEq.indexOf(100.5), 99);
        assertEquals(bdEq.indexOf(100.4), 99);
        assertEquals(bdEq.indexOf(70.5), 70);
        assertEquals(bdEq.indexOf(30.6), 30);
        assertEquals(bdEq.getLeftBoundary(23), 23.5, .1);
        assertEquals(bdEq.getRightBoundary(23), 24.5, .1);
        assertEquals(bdEq.getRightBoundary(99), 100.5, .1);
        double[] boundaries1 = new double[101];
        for (int i = 0; i < 101; i++)
            boundaries1[i] = i + 0.4;
        BucketsDescription1D bdEq1 = new BucketsDescription1D(boundaries1);
        assertFalse(bdEq.equals(bdEq1));
    }
}
