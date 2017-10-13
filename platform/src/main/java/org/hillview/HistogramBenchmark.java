package org.hillview;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.BucketsDescriptionEqSize;
import org.hillview.sketches.Histogram;
import org.hillview.sketches.HistogramSketch;
import org.hillview.table.ColumnDescription;
import org.hillview.table.FullMembership;
import org.hillview.table.Table;
import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.utils.HillviewLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark
 */
public class HistogramBenchmark {
    private static final ColumnDescription desc = new
            ColumnDescription("SQRT", ContentsKind.Double, true);

    /**
     * Generates a double array with every fifth entry missing
     */
    public static DoubleArrayColumn generateDoubleArray(final int size, final int max) {
        return generateDoubleArray(size, max, 5);
    }

    /**
     * Generates a double array with every skip entry missing
     */
    public static DoubleArrayColumn generateDoubleArray(final int size, final int max, int
            skip) {
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, Math.sqrt(i + 1) % max);
            if ((i % skip) == 0)
                col.setMissing(i);
        }
        return col;
    }

    static long time(Runnable runnable) {
        long start = System.nanoTime();
        runnable.run();
        long end = System.nanoTime();
        return end - start;
    }

    static String twoDigits(double d) {
        return String.format("%.2f", d);
    }

    static void runNTimes(Runnable runnable, int count, String message, int elemCount) {
        long[] times = new long[count];
        for (int i=0; i < count; i++) {
            long t = time(runnable);
            times[i] = t;
        }
        int minIndex = 0;
        for (int i=0; i < count; i++)
            if (times[i] < times[minIndex])
                minIndex = i;
        System.out.println(message);
        System.out.println("Time (ms),Melems/s,Percent slower");
        for (int i=0; i < count; i++) {
            double speed = (double)(elemCount) / (times[i] / 1000.0);
            double percent = 100 * ((double)times[i] - times[minIndex]) / times[minIndex];
            System.out.println((times[i]/(1000.0 * 1000.0)) + "," + twoDigits(speed) + "," + twoDigits(percent) + "%");
        }
    }


    public static void main(String[] args) {
        // Testing the performance of histogram computations
        final int bucketNum = 40;
        final int mega = 1024 * 1024;
        final int colSize = 100 * mega;
        final int runCount = Integer.parseInt(args[1]);

        HillviewLogger.initialize("/dev/null");

        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0, 100, bucketNum);
        final Histogram hist = new Histogram(buckDes);
        DoubleArrayColumn col = generateDoubleArray(colSize, 100);
        FullMembership fMap = new FullMembership(colSize);

        if (args[0].equals("simple")) {
            Runnable r = () -> hist.create(new ColumnAndConverter(col), fMap, 1.0);
            runNTimes(r, runCount, "Simple histogram", colSize);
        }
        List<IColumn> cols = new ArrayList<IColumn>();
            cols.add(col);
        ITable table = new Table(cols, fMap);
        ISketch<ITable, Histogram> sk = new HistogramSketch(buckDes, new ColumnAndConverterDescription(col
                .getName()));

        if (args[0].equals("noseparatethread")) {
            final IDataSet<ITable> ds = new LocalDataSet<ITable>(table, false);
            Runnable r = () -> ds.blockingSketch(sk);
            runNTimes(r, runCount, "Dataset histogram", colSize);
        }

        if (args[0].equals("separatethread")) {
            final IDataSet<ITable> lds = new LocalDataSet<ITable>(table);
            Runnable r = () -> lds.blockingSketch(sk);
            runNTimes(r, runCount, "Dataset histogram (separate thread)", colSize);
        }
    }
}
