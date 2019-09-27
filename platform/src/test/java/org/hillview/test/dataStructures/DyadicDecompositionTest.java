package org.hillview.test.dataStructures;

import org.hillview.dataset.api.Pair;
import org.hillview.sketches.DyadicHistogramBuckets;
import org.hillview.test.BaseTest;
import org.junit.Test;

import java.util.ArrayList;

public class DyadicDecompositionTest extends BaseTest {
    @Test
    public void testDyadicDecomposition() {
        long leftLeafIdx = 0;
        long rightLeafIdx = 10;

        ArrayList<Pair<Long, Long>> ret = DyadicHistogramBuckets.dyadicDecomposition(leftLeafIdx, rightLeafIdx);

        assert(ret.get(0).first == 0);
        assert(ret.get(0).second == 8);

        assert(ret.get(1).first == 8);
        assert(ret.get(1).second == 2);
    }
}
