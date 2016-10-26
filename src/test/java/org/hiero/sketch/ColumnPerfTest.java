package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.IntArrayColumn;
import org.junit.Test;

import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static junit.framework.TestCase.assertEquals;

public class ColumnPerfTest {
    @Test
    public void testColumnGetInt() {
        IntArrayColumn col;
        final int size = 1000000;
        final int testnum = 100;
        int temp = 0;
        ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, false);
        col = new IntArrayColumn(desc, size);
        for (int i=0; i < size; i++)
            col.set(i, i);
        System.out.println("Running function call test using lambdas");
        Consumer<Integer>  fcall =  tmp -> {
            for (int i = 0; i < size; i++)
                tmp += col.getInt(i);
        };
        TestUtil.runPerfTest(fcall,testnum);
        int Rcol[] = new int[size];
        for (int i=0; i < size; i++)
            Rcol[i]=i;
        Consumer<Integer> dcall = tmp -> {
            for (int i = 0; i < size; i++)
                tmp += Rcol[i];
        };
        System.out.println("running test on direct call using lambdas");
        TestUtil.runPerfTest(dcall, testnum);
    }
}
