package org.hillview.sketch;

import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.CategoryCentroidsSketch;
import org.hillview.sketches.Centroids;
import org.hillview.sketches.DistinctStrings;
import org.hillview.sketches.DistinctStringsSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CategoricalCentroidsSketchTest {

    private List<String> getUniqueStrings(ITable table, String colName) {
        DistinctStringsSketch sketch = new DistinctStringsSketch(table.getNumOfRows(), new String[]{colName});
        DistinctStrings distinctStrings = sketch.create(table).get(0);
        List<String> result = new ArrayList<>();
        distinctStrings.getStrings().forEach(result::add);
        return result;
    }

    @Test
    public void testFruits() {
        ITable table = TestTables.getCentroidTestTable();
        CategoryCentroidsSketch sketch = new CategoryCentroidsSketch(
                this.getUniqueStrings(table, "FruitType"),
                "FruitType",
                Arrays.asList("x", "y")
        );
        Centroids centroids = sketch.create(table);
        Assert.assertEquals(2, centroids.centroids.get(0, 0), Math.ulp(2));
        Assert.assertEquals(11, centroids.centroids.get(0, 1), Math.ulp(11));
        Assert.assertEquals(5, centroids.centroids.get(1, 0), Math.ulp(5));
        Assert.assertEquals(26, centroids.centroids.get(1, 1), Math.ulp(26));

        IDataSet<ITable> dataset = TestTables.makeParallel(table, 2);
        centroids = dataset.blockingSketch(sketch);
        Assert.assertEquals(2, centroids.centroids.get(0, 0), Math.ulp(2));
        Assert.assertEquals(11, centroids.centroids.get(0, 1), Math.ulp(11));
        Assert.assertEquals(5, centroids.centroids.get(1, 0), Math.ulp(5));
        Assert.assertEquals(26, centroids.centroids.get(1, 1), Math.ulp(26));
    }
}
