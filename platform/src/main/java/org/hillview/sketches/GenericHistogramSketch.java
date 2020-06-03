package org.hillview.sketches;

import org.hillview.sketches.results.Count;
import org.hillview.sketches.results.IHistogramBuckets;

public class GenericHistogramSketch
        extends GroupBySketch<Count,
                              EmptyWorkspace,
                              CountSketch,
                              Groups<Count>> {
    public GenericHistogramSketch(
            IHistogramBuckets buckets) {
        super(buckets, Groups::new, new CountSketch());
    }
}
