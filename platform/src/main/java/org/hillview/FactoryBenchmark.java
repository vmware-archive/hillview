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

public class FactoryBenchmark {
    public static void main(String[] args) throws IOException, InterruptedException {
        // Testing the performance of histogram computations
        final int bucketNum = 40;
        final int mega = 1024 * 1024;
        final int runCount = 10;
        final int colSize = 100 * mega;

        HillviewLogger.instance.setLogLevel(Level.OFF);
        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0, 100, bucketNum);
        PrintWriter writer = new PrintWriter("FactoryBenchmarkSparse", "UTF-8");

        for (int skipSize = 30; skipSize < 1000; skipSize += 20) {
            final DoubleArrayColumn col = HistogramBenchmark.generateDoubleArray(colSize, 100);
            ITable table = createSparseTable(colSize, col, skipSize);
            writer.print(skipSize + " , ");
            final IDataSet<ITable> ds = new LocalDataSet<ITable>(table, false);

            ISketch<ITable, Histogram> sk = new HistogramSketch(
                    buckDes, new ColumnAndConverterDescription(col.getName()), 1, 0);
            Runnable r = () -> ds.blockingSketch(sk);
            SamplingBenchmark.runNTimes(r, runCount, "Dataset histogram wo sampling", colSize, writer);
        }
        writer.close();
    }

    private static ITable createSparseTable(final int colSize, final IColumn col, final int skipSize) {
        SparseMembershipSet sMap = new SparseMembershipSet(colSize, colSize/skipSize);
        for (int i = 1; i < colSize; i += skipSize)
            sMap.add(i);
        sMap.seal();
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(col);
        return new Table(cols, sMap);
    }

    private static ITable createDenseTable(final int colSize, final IColumn col, final int skipSize) {
        DenseMembershipSet dMap = new DenseMembershipSet(colSize, colSize);
        for (int i = 1; i < colSize; i += skipSize)
            dMap.add(i);
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(col);
        return new Table(cols, dMap);
    }
}
