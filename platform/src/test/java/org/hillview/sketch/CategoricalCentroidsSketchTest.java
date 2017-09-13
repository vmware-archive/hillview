package org.hillview.sketch;

import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.CategoryCentroidsSketch;
import org.hillview.sketches.Centroids;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class CategoricalCentroidsSketchTest {
    @Test
    public void testFruits() {
        ITable table = TestTables.getCentroidTestTable();
        CategoryCentroidsSketch sketch = new CategoryCentroidsSketch(
                "FruitType",
                Arrays.asList("x", "y")
        );
        Centroids<String> centroids = sketch.create(table);
        HashMap<String, double[]> centroidsMap = centroids.computeCentroids();
        Assert.assertEquals(2, centroidsMap.get("Banana")[0], Math.ulp(2));
        Assert.assertEquals(11, centroidsMap.get("Banana")[1], Math.ulp(11));
        Assert.assertEquals(5, centroidsMap.get("Orange")[0], Math.ulp(5));
        Assert.assertEquals(26, centroidsMap.get("Orange")[1], Math.ulp(26));

        IDataSet<ITable> dataset = TestTables.makeParallel(table, 2);
        centroids = dataset.blockingSketch(sketch);
        centroidsMap = centroids.computeCentroids();
        Assert.assertEquals(2, centroidsMap.get("Banana")[0], Math.ulp(2));
        Assert.assertEquals(11, centroidsMap.get("Banana")[1], Math.ulp(11));
        Assert.assertEquals(5, centroidsMap.get("Orange")[0], Math.ulp(5));
        Assert.assertEquals(26, centroidsMap.get("Orange")[1], Math.ulp(26));
    }
}
