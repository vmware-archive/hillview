package org.hiero.sketch;

import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.ITable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hiero.sketch.DoubleArrayTest.generateDoubleArray;
import static org.hiero.sketch.IntArrayTest.generateIntArray;
import static org.hiero.sketch.IntArrayTest.getRandIntArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableTest {
    public static List<SmallTable> SplitTable(SmallTable bigTable, int fragmentSize) {
        int tableSize = bigTable.getNumOfRows();
        int numTables = (tableSize / fragmentSize) + 1;
        List<SmallTable> tableList = new ArrayList<SmallTable>(numTables);
        int start = 0;
        while (start < tableSize) {
            int thisFragSize = Math.min(fragmentSize, tableSize - start);
            IMembershipSet members = new SparseMembership(start, thisFragSize);
            tableList.add(bigTable.compress(members));
            start += fragmentSize;
        }
        return tableList;
    }

    public static SmallTable getIntTable(final int size, final int numCols) {
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        double exp = 1.0/numCols;
        final int range =  5*((int)Math.pow(size, exp));
        for (int i = 0; i < numCols; i++) {
            final String colName = "Column" + String.valueOf(i);
            columns.add(getRandIntArray(size, range, colName));
        }
        return new SmallTable(columns);
    }

    public static Table getRepIntTable(final int size, final int numCols) {
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        double exp = 0.8/numCols;
        final int range =  ((int)Math.pow(size, exp));
        for (int i = 0; i < numCols; i++) {
            final String colName = "Column" + String.valueOf(i);
            columns.add(getRandIntArray(size, range, colName));
        }
        final FullMembership full = new FullMembership(size);
        return new Table(columns, full);
    }

    @Test
    public void getTableTest(){
        final SmallTable leftTable = getIntTable(100, 2);
        assertNotNull(leftTable);
        //System.out.print(leftTable.toLongString());
    }

    @Test
    public void columnCompressTest() {
        final int size = 100;
        final IntArrayColumn col = generateIntArray(size);
        final FullMembership FM = new FullMembership(size);
        final LazyMembership PMD = new LazyMembership(FM, row -> (row % 2) == 0);
        final IColumn smallCol = col.compress(PMD);
        assertNotNull(smallCol);
    }

    @Test
    public void tableTest0() {
        final int size = 100;
        final int numCols =2;
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        columns.add(generateIntArray(size));
        columns.add(generateDoubleArray(size));
        FullMembership full = new FullMembership(size);
        LazyMembership partial = new LazyMembership(full, row -> (row % 2) == 0);
        Table myTable = new Table(columns, partial);
        assertEquals(myTable.toString(), "Table, 2 columns, 50 rows");
        ITable smallTable = myTable.compress();
        assertEquals(smallTable.toString(), "Table, 2 columns, 50 rows");
    }

    @Test
    public void tableTest1() {
        final int size = 100;
        final int numCols =2;
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        columns.add(generateIntArray(size));
        columns.add(generateDoubleArray(size));
        final FullMembership full = new FullMembership(size);
        final LazyMembership partial = new
                LazyMembership(full, row -> (row % 2) == 0);
        final Table myTable = new Table(columns, partial);
        assertEquals(myTable.toString(), "Table, 2 columns, 50 rows");
        HashSubSchema filter = new HashSubSchema();
        filter.add(columns.get(1).getDescription().name);
        ITable smallTable = myTable.compress(filter, partial);
        assertEquals(smallTable.toString(), "Table, 1 columns, 50 rows");
    }
}