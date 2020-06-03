package org.hillview.sketches;

import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.sketches.results.SampleSet;

public class HistogramQuantilesSketch extends
        GroupBySketch<SampleSet,
                      ColumnWorkspace<ReservoirSampleWorkspace>,
                      NumericSamplesSketch,
                      Groups<SampleSet>> {
    public HistogramQuantilesSketch(
            String quantilesColumn,
            int sampleCount,
            long seed,
            IHistogramBuckets buckets) {
        super(buckets, Groups::new,
                new NumericSamplesSketch(quantilesColumn, sampleCount, seed));
    }
}
