package org.hillview.test.dataStructures;

import org.hillview.dataset.api.Pair;
import org.hillview.sketches.DyadicHistogramBuckets;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class DyadicDecompositionTest extends BaseTest {
    @Test
    public void testDyadicDecomposition() {
        int leftLeafIdx = 0;
        int rightLeafIdx = 10;

        ArrayList<Pair<Integer, Integer>> ret = DyadicHistogramBuckets.dyadicDecomposition(leftLeafIdx, rightLeafIdx);

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
}
