package org.hillview.test;

import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.RandomSamplingSketch;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Test;

public class RandomSamplingSketchTest {

    @Test
    public void testRandomSampling() {
        ITable table = TestTables.getNdGaussianBlobs(10, 1000, 15, 0.1);
        IDataSet<ITable> dataset = TestTables.makeParallel(table, 500);
        int numSamples = 20;
        double samplingRate = ((double) numSamples) / table.getNumOfRows();
        RandomSamplingSketch sketch = new RandomSamplingSketch(samplingRate);
        SmallTable result = dataset.blockingSketch(sketch);
        System.out.println(String.format("Result has %d rows.", result.getNumOfRows()));
        result = result.compress(result.getMembershipSet().sample(numSamples));
        System.out.println(String.format("Resampled result has %d rows.", result.getNumOfRows()));
    }

}
