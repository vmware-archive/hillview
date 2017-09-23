package org.hillview.test;

import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.FilterMap;
import org.hillview.sketches.BasicColStatSketch;
import org.hillview.sketches.BasicColStats;
import org.hillview.table.*;
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class EqualityFilterTest {
    @Test
    public void testFilterSmallTable() {
        // Make a small table
        Table table = TestTables.testRepTable();

        // Make a filter and apply it
        EqualityFilter equalityFilter = new EqualityFilter("Name", "Ed");
        FilterMap filterMap = new FilterMap(equalityFilter);
        ITable result = filterMap.apply(table);

        // Assert number of rows are as expected
        Assert.assertEquals(1, result.getNumOfRows());

        // Make sure the rows are correct
        IRowIterator it = result.getMembershipSet().getIterator();
        int row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals("Ed", result.getColumn("Name").getString(row));
            row = it.getNextRow();
        }

        // Same process for Mike.
        equalityFilter = new EqualityFilter("Name", "Mike");
        filterMap = new FilterMap(equalityFilter);
        result = filterMap.apply(table);

        Assert.assertEquals(2, result.getNumOfRows());

        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals("Mike", result.getColumn("Name").getString(row));
            row = it.getNextRow();
        }
    }

    @Test
    public void testFilterIntegers() {
        // Make a small table
        Table table = TestTables.testRepTable();

        // Make a filter and apply it
        EqualityFilter equalityFilter = new EqualityFilter("Age", 10);
        FilterMap filterMap = new FilterMap(equalityFilter);
        ITable result = filterMap.apply(table);

        // Assert number of rows are as expected
        Assert.assertEquals(4, result.getNumOfRows());

        // Make a filter and apply it
        equalityFilter = new EqualityFilter("Age", 40);
        filterMap = new FilterMap(equalityFilter);
        result = filterMap.apply(table);

        // Assert number of rows are as expected
        Assert.assertEquals(2, result.getNumOfRows());
    }

    @Test
    public void testFilterLargeStringTable(){
        // Make a larger ITable
        int size = 500;
        int count = 17;
        String[] possibleNames = {"John", "Robert", "Ed", "Sam", "Ned", "Jaime", "Rickard"};
        String name = "Varys"; // The name we're counting
        ITable table = TestTables.testLargeStringTable(size, possibleNames, count, name);

        // Make the filter map
        EqualityFilter equalityFilter = new EqualityFilter("Name", name);
        FilterMap filterMap = new FilterMap(equalityFilter);

        // Apply the filter map
        ITable result = filterMap.apply(table);

        // Assert that the number of occurrences is correct.
        Assert.assertEquals(count, result.getNumOfRows());

        // Assert that the correct rows are filtered. (They should all have the same name.)
        IRowIterator it = result.getMembershipSet().getIterator();
        int row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals(name, result.getColumn("Name").getString(row));
            row = it.getNextRow();
        }
    }

    @Test
    public void testStringDataset() {
        // Make a quite large ITable
        int bigSize = 10000;
        int count = 42;
        String[] possibleNames = {"John", "Robert", "Ed", "Sam", "Ned", "Jaime", "Rickard"};
        String name = "Varys";
        ITable bigTable = TestTables.testLargeStringTable(bigSize, possibleNames, count, "Varys");

        // Convert it to an IDataset
        IDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);

        // Make the filter map
        EqualityFilter equalityFilter = new EqualityFilter("Name", name);
        FilterMap filterMap = new FilterMap(equalityFilter);

        // Apply the map to the IDataset.
        IDataSet<ITable> result = all.blockingMap(filterMap);

        // Count the number of rows in the resulting IDataset with a BasicColStatsSketch
        SortedStringsConverter converter = new SortedStringsConverter(possibleNames, 0, 50);
        BasicColStatSketch b = new BasicColStatSketch(
                new ColumnNameAndConverter("Name", converter));
        BasicColStats bcs = result.blockingSketch(b);

        // The sketch should have counted 'count' 'name's in the IDataset.
        Assert.assertEquals(count, bcs.getRowCount());
    }
}
