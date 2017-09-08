package org.hillview.sketch;

import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.ToCatMap;
import org.hillview.sketches.DistinctStrings;
import org.hillview.sketches.DistinctStringsSketch;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class ToCatMapTest {
    @Test
    public void testToCatMap() {
        ITable table = TestTables.testRepTable();
        IMap<ITable, ITable> map = new ToCatMap("Name");
        ITable result = map.apply(table);

        Assert.assertTrue(result.getColumn("Name (Cat.)").getDescription().kind == ContentsKind.Category);
        IRowIterator rowIt = result.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            Assert.assertEquals(
                    result.getColumn("Name").getString(row),
                    result.getColumn("Name (Cat.)").getString(row)
            );
            row = rowIt.getNextRow();
        }
    }

    @Test
    public void testToCatMapBig() {
        ITable table = TestTables.testRepTable();
        IDataSet<ITable> bigTable = TestTables.makeParallel(table, 3);
        IMap<ITable, ITable> map = new ToCatMap("Name");

        IDataSet<ITable> result = bigTable.blockingMap(map);

        DistinctStringsSketch uss1 = new DistinctStringsSketch(100, "Name");
        DistinctStringsSketch uss2 = new DistinctStringsSketch(100, "Name (Cat.)");
        DistinctStrings ds1 = result.blockingSketch(uss1);
        DistinctStrings ds2 = result.blockingSketch(uss2);
        Set<String> strings1 = new HashSet<String>();
        for (String s : ds1.getStrings()) {
            strings1.add(s);
        }
        Set<String> strings2 = new HashSet<String>();
        for (String s : ds2.getStrings()) {
            strings2.add(s);
        }

        Assert.assertTrue(strings1.equals(strings2));
    }
}
