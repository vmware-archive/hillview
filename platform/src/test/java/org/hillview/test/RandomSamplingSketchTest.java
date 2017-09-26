package org.hillview.test;

import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.RandomSampling;
import org.hillview.sketches.RandomSamplingSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.MetricMDS;
import org.hillview.utils.TestTables;
import org.hillview.utils.TestUtils;
import org.jblas.DoubleMatrix;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class RandomSamplingSketchTest {

    @Test
    public void testSegmentation() {
        try {
            ITable table = TestUtils.loadTableFromCSV("../data", "segmentation.csv", "segmentation.schema");
            List<String> numericColNames = TestUtils.getNumericColumnNames(table);
            RandomSamplingSketch sketch = new RandomSamplingSketch(50, numericColNames);
            IDataSet<ITable> dataset = TestTables.makeParallel(table, 40);
            RandomSampling sample = dataset.blockingSketch(sketch);
            MetricMDS mds = new MetricMDS(sample.data);
            DoubleMatrix proj = mds.computeEmbedding(1);
            proj.print();
        } catch (IOException e) {
            System.out.println("Skipping test, because segmentation data is not present.");
        }
    }

    @Test
    public void testMNIST() {
        try {
            ITable table = TestUtils.loadTableFromCSV("../data", "mnist.csv", "mnist.schema");
            List<String> numericColNames = TestUtils.getNumericColumnNames(table);
            RandomSamplingSketch sketch = new RandomSamplingSketch(20, numericColNames);
            IDataSet<ITable> dataset = TestTables.makeParallel(table, 40);
            RandomSampling sample = dataset.blockingSketch(sketch);
            MetricMDS mds = new MetricMDS(sample.data);
            DoubleMatrix proj = mds.computeEmbedding(1);
            proj.print();
        } catch (IOException e) {
            System.out.println("Skipping test, because MNIST data is not present.");
        }
    }
}
