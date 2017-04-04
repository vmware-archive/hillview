package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.ExactFreqSketch;
import org.hiero.sketch.spreadsheet.FreqKList;
import org.hiero.sketch.spreadsheet.FreqKSketch;
import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.TestTables;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class ExactFreqSketchTest {

    public String getFrequencies(ITable table, int maxSize) {
        FreqKSketch fk = new FreqKSketch(table.getSchema(), maxSize);
        FreqKList fkList = fk.create(table);
        StringBuilder sb = new StringBuilder();
        List keys = new ArrayList<RowSnapshot>(fkList.hMap.keySet());
        ExactFreqSketch ef = new ExactFreqSketch(table.getSchema(), keys);
        HashMap<RowSnapshot, Integer> hMap = ef.create(table);
        hMap.forEach((r,s) -> sb.append(r.toString()).append(": ").append(s)
                .append(". (").append(fkList.hMap.get(r)).append("-")
                                .append(fkList.hMap.get(r) + fkList.GetErrBound())
                                .append(")").append("\n"));
        return sb.toString();
    }

    @Test
    public void EFSTest1() {
        Table t = TestTables.testRepTable();
        int maxSize = 10;
        String s = getFrequencies(t, maxSize);
        System.out.println(s);
    }

    @Test
    public void EFSTest2() {
        SmallTable t = TestTables.getHeavyIntTable(2,10000,2,14);
        int maxSize = 10;
        String s = getFrequencies(t, maxSize);
        System.out.println(s);
    }

    @Test
    public void EFSTest3() {
        SmallTable t = TestTables.getHeavyIntTable(2,10000,1.4,20);
        int maxSize = 20;
        String s = getFrequencies(t, maxSize);
        System.out.println(s);
    }

}
