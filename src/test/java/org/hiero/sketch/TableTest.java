package org.hiero.sketch;

import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IColumn;
import org.junit.Test;

import static org.hiero.sketch.DoubleArrayTest.generateDoubleArray;
import static org.hiero.sketch.IntArrayTest.generateIntArray;
import static org.junit.Assert.assertEquals;

public class TableTest {

    @Test
    public void columnCompressTest() {
        final int size = 100;
        final IntArrayColumn col = generateIntArray(size);
        final FullMembership FM = new FullMembership(size);
        final PartialMembershipDense PMD = new PartialMembershipDense(FM, row -> (row % 2) == 0);
        final IColumn smallCol = col.compress(PMD);
    }

    @Test
    public void tableTest0() {
        final int size = 100;
        final int numCols =2;
        final IColumn[] columns = new IColumn[numCols];
        columns[0] = generateIntArray(size);
        columns[1] = generateDoubleArray(size);
        final Schema mySchema = new Schema();
        for (int i=0; i< numCols; i++) {
            mySchema.append(columns[i].getDescription());
        }
        final FullMembership full = new FullMembership(size);
        final PartialMembershipDense partial = new PartialMembershipDense(full, row -> (row % 2) == 0);
        final Table myTable = new Table(mySchema, columns, partial);
        assertEquals(myTable.toString(), "Table, 2 columns, 50 rows");
        final Table smallTable = myTable.compress();
        assertEquals(smallTable.toString(), "Table, 2 columns, 50 rows");
    }

    @Test
    public void tableTest1() {
        final int size = 100;
        final int numCols =2;
        final IColumn[] columns = new IColumn[numCols];
        columns[0] = generateIntArray(size);
        columns[1] = generateDoubleArray(size);
        final Schema mySchema = new Schema();
        for (int i=0; i< numCols; i++) {
            mySchema.append(columns[i].getDescription());
        }
        final FullMembership full = new FullMembership(size);
        final PartialMembershipDense partial = new
                PartialMembershipDense(full, row -> (row % 2) == 0);
        final Table myTable = new Table(mySchema, columns, partial);
        assertEquals(myTable.toString(), "Table, 2 columns, 50 rows");
        final HashSubSchema filter = new HashSubSchema();
        filter.add(columns[1].getDescription().name);
        final Table smallTable = myTable.compress(filter);
        assertEquals(smallTable.toString(), "Table, 1 columns, 50 rows");
    }
}
