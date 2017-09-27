package org.hillview.test;

import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.LAMPMap;
import org.hillview.sketches.RandomSampling;
import org.hillview.sketches.RandomSamplingSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.MetricMDS;
import org.hillview.utils.TestTables;
import org.hillview.utils.TestUtils;
import org.jblas.DoubleMatrix;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LAMPMapTest {

    private void testLAMPMap(ITable table, int numSamples, int fragmentSize) {
        IDataSet<ITable> dataset = TestTables.makeParallel(table, fragmentSize);
        List<String> colNames = TestUtils.getNumericColumnNames(table);
        RandomSamplingSketch sketch = new RandomSamplingSketch(numSamples, colNames, false);
        RandomSampling sample = dataset.blockingSketch(sketch);
        DoubleMatrix ndControlPoints = BlasConversions.toDoubleMatrix(sample.table, colNames);
        MetricMDS mds = new MetricMDS(ndControlPoints);
        DoubleMatrix proj = mds.computeEmbedding(3);

        System.out.println("\nMDS projection:");
        System.out.println("\tMin x: " + proj.getColumn(0).min());
        System.out.println("\tMax x: " + proj.getColumn(0).max());
        System.out.println("\tMin y: " + proj.getColumn(1).min());
        System.out.println("\tMax y: " + proj.getColumn(1).max());

        List<String> newColNames = new ArrayList<String>();
        newColNames.add("LAMP1");
        newColNames.add("LAMP2");
        LAMPMap map = new LAMPMap(ndControlPoints, proj, colNames, newColNames);
        ITable result = map.apply(table);
        IDataSet<ITable> datasetResult = dataset.blockingMap(map);

        DoubleMatrix lampProjection = BlasConversions.toDoubleMatrix(result, newColNames);
        System.out.println("\nLAMP projection:");
        System.out.println("\tMin x: " + lampProjection.getColumn(0).min());
        System.out.println("\tMax x: " + lampProjection.getColumn(0).max());
        System.out.println("\tMin y: " + lampProjection.getColumn(1).min());
        System.out.println("\tMax y: " + lampProjection.getColumn(1).max());
        System.out.println("Number of NaNs: " + lampProjection.isNaN().sum());
    }

    @Test
    public void testBlobs() {
        ITable table = TestTables.getNdGaussianBlobs(10, 200, 15, 0.05);
        this.testLAMPMap(table, 20, 40);
    }

    @Test
    public void testMNIST() {
        try {
            ITable table = TestUtils.loadTableFromCSV("../data", "mnist.csv", "mnist.schema");
            this.testLAMPMap(table, 20, 40);
        } catch (IOException e) {
            System.out.println("Skipping test because MNIST data is not present.");
        }
    }

    @Test
    public void testSegmentation() {
        try {
            ITable table = TestUtils.loadTableFromCSV("../data", "segmentation.csv", "segmentation.schema");
            this.testLAMPMap(table, 20, 40);
        } catch (IOException e) {
            System.out.println("Skipping test because MNIST data is not present.");
        }
    }
}
