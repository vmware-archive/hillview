package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.ColumnOrientation;
import org.hiero.sketch.spreadsheet.QuantileList;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.ColumnSortOrder;
import org.hiero.sketch.table.ListComparator;
import org.hiero.sketch.table.Table;
import org.junit.Test;

import java.util.ArrayList;

import static junit.framework.TestCase.assertTrue;

public class TableDataSetTest {
    @Test
    public void localDataSetTest() {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final Table randTable = TableTest.getIntTable(size, numCols);
        ColumnSortOrder cso = new ColumnSortOrder();
        for (String colName: randTable.schema.getColumnNames()) {
            cso.append(new ColumnOrientation(randTable.schema.getDescription(colName), true));
        }
        final QuantileSketch qSketch = new QuantileSketch(cso, resolution);
        final LocalDataSet<Table> ld = new LocalDataSet<Table>(randTable);
        final QuantileList ql = ld.blockingSketch(qSketch);
        ListComparator comp = cso.getComparator(ql.quantile);
        for(int i = 0; i < ql.getQuantileSize() -1; i++ )
            assertTrue(comp.compare(i,i+1) <= 0);
        //System.out.println(ql);
    }

    @Test
    public void parallelDataSetTest() {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final Table randTable1 = TableTest.getIntTable(size, numCols);
        final Table randTable2 = TableTest.getIntTable(size, numCols);
        ColumnSortOrder cso = new ColumnSortOrder();
        for (String colName: randTable1.schema.getColumnNames()) {
            cso.append(new ColumnOrientation(randTable1.schema.getDescription(colName), true));
        }

        final LocalDataSet<Table> ld1 = new LocalDataSet<Table>(randTable1);
        final LocalDataSet<Table> ld2 = new LocalDataSet<Table>(randTable2);
        final ArrayList<IDataSet<Table>> elems = new ArrayList<IDataSet<Table>>(2);
        elems.add(ld1);
        elems.add(ld2);
        final ParallelDataSet<Table> par = new ParallelDataSet<Table>(elems);
        final QuantileSketch qSketch = new QuantileSketch(cso, resolution);
        final QuantileList r = par.blockingSketch(qSketch);
        ListComparator comp = cso.getComparator(r.quantile);
        for(int i = 0; i < r.getQuantileSize() -1; i++ )
            assertTrue(comp.compare(i,i+1) <= 0);
        //System.out.println(r);
    }

    /*
    private class Sum implements ISketch<int[], Integer> {
        @Override
        public Integer zero() {
            return 0;
        }

        @Override
        public Integer add(final Integer left, final Integer right) {
            return left + right;
        }

        @Override
        public Observable<PartialResult<Integer>> create(final int[] data) {
            final int parts = 10;
            return Observable.range(0, parts).map(index -> {
                final int partSize = data.length / parts;
                final int left = partSize * index;
                final int right = (index == (parts - 1)) ? data.length : (left + partSize);
                int sum1 = 0;
                for (int i=left; i < right; i++)
                    sum1 += data[i];
                return new PartialResult<Integer>(1.0 / parts, sum1);
            });
        }
    }

    private final int largeSize = 10 * 1024 * 1024;

    private IDataSet<int[]> createLargeDataset(final boolean separateThread) {
        final int[] data = new int[this.largeSize];
        for (int i=0; i < this.largeSize; i++)
            data[i] = ((i % 10) == 0) ? 0 : i;
        return new LocalDataSet<int[]>(data, separateThread);
    }

    @Test
    public void largeDataSetTest() {
        final IDataSet<int[]> ld = this.createLargeDataset(false);
        final int result = ld.blockingSketch(new Sum());
        int sum = 0;
        for (int i=0; i < this.largeSize; i++)
            sum += ((i % 10) == 0) ? 0 : i;
        assertEquals(result, sum);
    }

    @Test
    public void separateThreadDataSetTest() {
        final IDataSet<int[]> ld = this.createLargeDataset(true);
        final int result = ld.blockingSketch(new Sum());
        int sum = 0;
        for (int i=0; i < this.largeSize; i++)
            sum += ((i % 10) == 0) ? 0 : i;
        assertEquals(result, sum);
    }

    @Test
    public void unsubscriptionTest() {
        final IDataSet<int[]> ld = this.createLargeDataset(true);
        final Observable<PartialResult<Integer>> pr = ld.sketch(new Sum());
        pr.subscribe(new Subscriber<PartialResult<Integer>>() {
            private int count = 0;
            private double done = 0.0;

            @Override
            public void onCompleted() {
                fail("Unreachable");
            }

            @Override
            public void onError(final Throwable throwable) {
                fail("Unreachable");
            }

            @Override
            public void onNext(final PartialResult<Integer> pr) {
                this.done += pr.deltaDone;
                this.count++;
                if (this.count == 3)
                    this.unsubscribe();
                else
                    assertEquals(this.done, 0.1 * this.count, 1e-3);
            }
        });
    }
    */
}
