package org.hillview.sketches;

import org.hillview.sketches.results.Count;
import org.hillview.sketches.results.IHistogramBuckets;

public class Histogram2DSketch extends GroupBySketch<
        Groups<Count>,
        GroupByWorkspace<EmptyWorkspace>,
        GenericHistogramSketch,
        Groups<Groups<Count>>> {

    public Histogram2DSketch(
            IHistogramBuckets buckets0,
            IHistogramBuckets buckets1) {
        super(buckets1, Groups::new,
                new GenericHistogramSketch(buckets0));
    }
}
