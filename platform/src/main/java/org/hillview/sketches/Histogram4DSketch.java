package org.hillview.sketches;

import org.hillview.sketches.results.Count;
import org.hillview.sketches.results.IHistogramBuckets;

public class Histogram4DSketch extends GroupBySketch<
        Groups<Groups<Groups<Count>>>,
        GroupByWorkspace<GroupByWorkspace<GroupByWorkspace<EmptyWorkspace>>>,
        Histogram3DSketch,
        Groups<Groups<Groups<Groups<Count>>>>> {

    public Histogram4DSketch(
            IHistogramBuckets buckets0,
            IHistogramBuckets buckets1,
            IHistogramBuckets buckets2,
            IHistogramBuckets buckets3) {
        super(buckets3, Groups::new,
                new Histogram3DSketch(buckets0, buckets1, buckets2));
    }
}
