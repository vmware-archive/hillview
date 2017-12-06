package org.hillview;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.BucketsDescriptionEqSize;
import org.hillview.sketches.Histogram;
import org.hillview.sketches.HistogramSketch;
import org.hillview.table.Table;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.membership.DenseMembershipSet;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.membership.SparseMembershipSet;
import org.hillview.utils.HillviewLogger;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class SamplingBenchmark {
    public static void main(String[] args) throws IOException, InterruptedException {
        // Testing the performance of histogram computations
        final int bucketNum = 40;
        final int mega = 1024 * 1024;
        final int runCount = 10;
        HillviewLogger.instance.setLogLevel(Level.OFF);
        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0, 100, bucketNum);
        PrintWriter writer = new PrintWriter("SamplingBenchmarkFull", "UTF-8");

        for (int elements = 10 * mega; elements < 101 * mega; elements += (10 * mega)) {
            final DoubleArrayColumn col = HistogramBenchmark.generateDoubleArray(elements, 100);
            ITable table = createTable(elements, col);
            writer.println("averaging " + runCount + " runs, with " + elements + " elements");
            writer.println("rate,  elements, min, max, mean (ms)");
            final IDataSet<ITable> ds = new LocalDataSet<ITable>(table, false);
            for (double rate = 0.01; rate < 0.2; rate += 0.01) {
                ISketch<ITable, Histogram> sk = new HistogramSketch(
                        buckDes, new ColumnAndConverterDescription(col.getName()), rate, 0);
                Runnable r = () -> ds.blockingSketch(sk);
                writer.print(rate + " , ");
                runNTimes(r, runCount, "Dataset histogram wo sampling", elements, writer);
            }
            ISketch<ITable, Histogram> sk = new HistogramSketch(
                    buckDes, new ColumnAndConverterDescription(col.getName()), 1, 0);
            Runnable r = () -> ds.blockingSketch(sk);
            writer.print("1 , ");
            runNTimes(r, runCount, "Dataset histogram wo sampling", elements, writer);
            System.out.println(elements);
        }
        writer.close();
    }

    private static ITable createTable(final int colSize, final IColumn col) {
        FullMembershipSet fMap = new FullMembershipSet(colSize);
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(col);
        return new Table(cols, fMap);
    }

    private static ITable createSparseTable(final int colSize, final IColumn col) {
        SparseMembershipSet sMap = new SparseMembershipSet(colSize, colSize);
        for (int i = 1; i < colSize; i++)
            sMap.add(i);
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(col);
        return new Table(cols, sMap);
    }

    private static ITable createDenseTable(final int colSize, final IColumn col) {
        DenseMembershipSet dMap = new DenseMembershipSet(colSize, colSize);
        for (int i = 1; i < colSize; i+=2)
            dMap.add(i);
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(col);
        return new Table(cols, dMap);
    }

    static void runNTimes(Runnable runnable, int count, String message, int elemCount, PrintWriter writer) throws IOException {
        long[] times = new long[count];
        for (int i=0; i < count; i++) {
            long t = HistogramBenchmark.time(runnable);
            times[i] = t;
            System.out.print(".");
        }
        int minIndex = 0;
        for (int i=0; i < count; i++)
            if (times[i] < times[minIndex])
                minIndex = i;
        double max = 0, mean = 0;
        double min = Double.MAX_VALUE;
        for (int i=0; i < count; i++) {
            mean += times[i];
            if (max < times[i]) max = times[i];
            if (min > times[i]) min = times[i];
        }
        double ml = 1000000;
        writer.println(elemCount + " , " + min / ml +  " , " + max / ml + " , " + mean /(ml * count) );
    }
}
