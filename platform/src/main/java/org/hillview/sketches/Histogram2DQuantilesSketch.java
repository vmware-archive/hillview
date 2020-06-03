package org.hillview.sketches;

import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.sketches.results.SampleSet;

public class Histogram2DQuantilesSketch
        extends GroupBySketch<Groups<SampleSet>,
                              GroupByWorkspace<ColumnWorkspace<ReservoirSampleWorkspace>>,
                              HistogramQuantilesSketch,
                              Groups<Groups<SampleSet>>> {
    public Histogram2DQuantilesSketch(
            String column,
            int quantileCount,
            long seed,
            IHistogramBuckets buckets0,
            IHistogramBuckets buckets1) {
        super(buckets1, Groups::new,
                new HistogramQuantilesSketch(column, quantileCount, seed, buckets0));
    }
}
