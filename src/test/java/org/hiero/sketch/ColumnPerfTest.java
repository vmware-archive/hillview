package org.hiero.sketch;

/**
 * Created by uwieder on 10/14/16.
 */


import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.ContentsKind;
import org.hiero.sketch.table.IntArrayColumn;
import org.junit.Test;
import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;
public class ColumnPerfTest {
    @Test
    public void ColumnPerfTest() {
        IntArrayColumn col;
        final int size = 1000000;
        final int testnum = 100;
        int temp=0;

        long[] results = new long[testnum];  // will hold the timing of IntArrayColumn
        long[] Cresults = new long[testnum]; // will hold teh timing of a comparable simple int array

        ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, false);
        col = new IntArrayColumn(desc, size);
        for (int i=0; i < size; i++)
            col.set(i, i);
        temp =0;
        for (int j=0; j<testnum; j++) {

            long startTime = System.nanoTime();

            for (int i = 0; i < size; i++) {
                temp += col.getInt(i)+j;
            }

            long endTime = System.nanoTime();
            results[j] = endTime - startTime;
        }
        System.out.println(temp); //here to prevent the compiler from throwing away the variable temp




        int Rcol[] = new int[size];
        for (int i=0; i < size; i++)
            Rcol[i]=i;
        temp = 0;
        for (int j=0; j<testnum; j++) {
            long startTime = System.nanoTime();

            for (int i = 0; i < size; i++) {
                temp += Rcol[i]+j;
            }

            long endTime = System.nanoTime();
            Cresults[j] = endTime - startTime;
        }
        System.out.println(temp);
        System.out.println("Comaparing function call to Direct manimulation of Array");
        TestUtil.Percentiles(results);
        TestUtil.Percentiles(Cresults);



    }
}
