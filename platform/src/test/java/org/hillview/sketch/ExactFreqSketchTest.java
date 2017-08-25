package org.hillview.sketch;

import org.hillview.dataset.api.Pair;
import org.hillview.sketches.ExactFreqSketch;
import org.hillview.sketches.FreqKList;
import org.hillview.sketches.FreqKSketch;
import org.hillview.table.RowSnapshot;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;


public class ExactFreqSketchTest {
    @SuppressWarnings("SuspiciousMethodCalls")
    // Idea is complaining about the hMap.get calls below,
    // but it also complains if I add explicit casts.
    private void getFrequencies(ITable table, int maxSize) {
        FreqKSketch fk = new FreqKSketch(table.getSchema(), maxSize);
        FreqKList fkList = fk.create(table);
        ExactFreqSketch ef = new ExactFreqSketch(table.getSchema(), fkList);
        FreqKList exactList = ef.create(table);
        int size = 10;
        List<Pair<RowSnapshot, Integer>> topList = exactList.getTop(size);
        for (int i =1; i < topList.size(); i++)
            assertTrue(topList.get(i-1).second >= topList.get(i).second);
        exactList.filter();
        exactList.getList().forEach(rss ->
                assertTrue(exactList.hMap.get(rss) >= fkList.totalRows/fkList.maxSize ));
    }

    @Test
    public void EFSTest1() {
        Table t1 = TestTables.testRepTable();
        int maxSize1 = 20;
        getFrequencies(t1, maxSize1);
        SmallTable t2 = TestTables.getHeavyIntTable(2,10000,2,14);
        int maxSize2 = 20;
        getFrequencies(t2, maxSize2);
        SmallTable t3 = TestTables.getHeavyIntTable(2,10000,1.4,20);
        int maxSize3 = 30;
        getFrequencies(t3, maxSize3);
    }
}
