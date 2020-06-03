package org.hillview.sketches;

import org.hillview.sketches.results.Count;
import org.hillview.sketches.results.IHistogramBuckets;

public class Histogram3DSketch extends GroupBySketch<
        Groups<Groups<Count>>,
        GroupByWorkspace<GroupByWorkspace<EmptyWorkspace>>,
        Histogram2DSketch,
        Groups<Groups<Groups<Count>>>> {

    public Histogram3DSketch(
            IHistogramBuckets buckets0,
            IHistogramBuckets buckets1,
            IHistogramBuckets buckets2) {
        super(buckets2, Groups::new,
                new Histogram2DSketch(buckets0, buckets1));
    }
}
