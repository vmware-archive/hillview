package org.hillview.sketch;

import org.hillview.sketches.ExactFreqSketch;
import org.hillview.sketches.FreqKList;
import org.hillview.sketches.FreqKSketch;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Test;


public class ExactFreqSketchTest {
    @SuppressWarnings("SuspiciousMethodCalls")
    // Idea is complaining about the hMap.get calls below,
    // but it also complains if I add explicit casts.
    private String getFrequencies(ITable table, int maxSize) {
        FreqKSketch fk = new FreqKSketch(table.getSchema(), maxSize);
        FreqKList fkList = fk.create(table);
        StringBuilder sb = new StringBuilder();
        ExactFreqSketch ef = new ExactFreqSketch(table.getSchema(), fkList.hMap.keySet());
        ExactFreqSketch.Frequencies hMap = ef.create(table);
        hMap.count.forEach(
                (r,s) -> sb.append(r.toString()).append(": ").append(s)
                           .append(". (").append(fkList.hMap.get(r)).append("-")
                           .append(fkList.hMap.get(r) + fkList.getErrBound())
                           .append(")").append("\n"));
        return sb.toString();
    }

    @Test
    public void EFSTest1() {
        Table t = TestTables.testRepTable();
        int maxSize = 10;
        String s = getFrequencies(t, maxSize);
        //System.out.println(s);
    }

    @Test
    public void EFSTest2() {
        SmallTable t = TestTables.getHeavyIntTable(2,10000,2,14);
        int maxSize = 10;
        String s = getFrequencies(t, maxSize);
        //System.out.println(s);
    }

    @Test
    public void EFSTest3() {
        SmallTable t = TestTables.getHeavyIntTable(2,10000,1.4,20);
        int maxSize = 20;
        String s = getFrequencies(t, maxSize);
        //System.out.println(s);
    }
}
