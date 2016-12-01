package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.OrderedColumn;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.FullMembership;
import org.hiero.sketch.table.Quantiles;
import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.IColumn;
import org.junit.Test;

import java.util.*;

import static org.hiero.sketch.DoubleArrayTest.generateDoubleArray;
import static org.hiero.sketch.IntArrayTest.generateIntArray;

public class QuantileTest {


    @Test
    public void TestQZero() {
        /* Create a table with two columns, one integer and the other double */
        final int size = 100000;
        final int numCols = 2;
        final IColumn[] columns = new IColumn[numCols];
        columns[0] = generateIntArray(size);
        columns[1] = generateDoubleArray(size);
        final Schema mySchema = new Schema();
        for (int i = 0; i < numCols; i++) {
            mySchema.append(columns[i].getDescription());
        }
        final FullMembership full = new FullMembership(size);
        final Table myTable = new Table(mySchema, columns, full);
        final List<OrderedColumn> sortOrder = new ArrayList<>();
        sortOrder.add(new OrderedColumn("SQRT", false));
        sortOrder.add(new OrderedColumn("Identity", true));
        final QuantileSketch qSketch = new QuantileSketch(myTable, sortOrder);
        qSketch.getQuantile(100).printTable();
    }

    @Test
    public void TestQOne() {
        final int inpSize = 100;
        final int resolution = 5;
        final int[] input = new int[inpSize];
        final Random rn = new Random();
        for (int i = 0; i < inpSize; i++) {
            input[i] = rn.nextInt(inpSize);
        }
        final Comparator<Integer> comp = MyCompare.instance;
        final Quantiles qn = new Quantiles(input, comp);
        @SuppressWarnings("UnusedAssignment") final Integer[] qtiles= qn.getQuantiles(resolution);
        Arrays.sort(input);
        for(int i =0; i < resolution; i++) {
            @SuppressWarnings("UnusedAssignment") final int j = (inpSize * i) / resolution;
            /*
            System.out.printf("Quantile %d:  %d (%d) %n", i, qtiles[i], qtiles[i] - input[j]);
             */
        }
    }

    @Test
    public void TestQTwo() {
        final int inpSize = 100;
        final int resolution = 10;
        final int[] input = new int[inpSize];
        final Random rn = new Random();
        for (int i = 0; i < inpSize; i++) {
            input[i] = rn.nextInt(inpSize);
        }
        final Comparator<Integer> comp = MyCompare.instance;
        final Quantiles qn = new Quantiles(input, comp);
        final Integer[] qtiles= qn.getQuantiles(resolution);
        Arrays.sort(input);
        int j =0;
        for(int i =0; i < resolution; i++) {
            while(input[j] < qtiles[i])
                j++;
            @SuppressWarnings("UnusedAssignment") final double k = ((float) j * resolution) / inpSize;
            /*
            System.out.printf("Quantile %d (%f) %n", i, k);
             */
        }
    }
}