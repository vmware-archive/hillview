package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.IntArrayColumn;
import org.junit.Test;

import java.util.function.Consumer;

public class ColumnPerfTest {
    @Test
    public void testColumnGetInt() {
        final IntArrayColumn col;
        final int size = 1000000;
        final int testnum = 10;
        final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, false);

        col = new IntArrayColumn(desc, size);
        for (int i=0; i < size; i++)
            col.set(i, i);
        final Consumer<Integer>  fcall = tmp -> {
            for (int i = 0; i < size; i++)
                tmp += col.getInt(i);
        };
        TestUtil.runPerfTest(fcall,testnum);
        final int[] Rcol = new int[size];
        for (int i=0; i < size; i++)
            Rcol[i]=i;
        final Consumer<Integer> dcall = tmp -> {
            for (int i = 0; i < size; i++)
                tmp += Rcol[i];
        };
        TestUtil.runPerfTest(dcall, testnum);
    }
}
