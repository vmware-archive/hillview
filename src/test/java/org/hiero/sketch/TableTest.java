package org.hiero.sketch;

import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IColumn;
import org.junit.Test;

import static org.hiero.sketch.DoubleArrayTest.generateDoubleArray;
import static org.hiero.sketch.IntArrayTest.generateIntArray;

public class TableTest {

    @Test
    public void ColumnCompressTest() {
        int size = 100;
        IntArrayColumn col = generateIntArray(size);
        final FullMembership FM = new FullMembership(size);
        final PartialMembershipDense PMD = new PartialMembershipDense(FM, row -> (row % 2) == 0);
        IColumn smallCol = col.compress(PMD);
    }

    @Test
    public void TableTest0() {
        int size = 100;
        int numCols =2;
        IColumn[] columns = new IColumn[numCols];
        columns[0] = generateIntArray(size);
        columns[1] = generateDoubleArray(size);
        Schema mySchema = new Schema();
        for (int i=0; i< numCols; i++) {
            mySchema.append(columns[i].getDescription());
        }
        final FullMembership FM = new FullMembership(size);
        final PartialMembershipDense PMD = new PartialMembershipDense(FM, row -> (row % 2) == 0);
        Table myTable = new Table(mySchema, columns, FM);
        Table smallTable = myTable.compress();
    }


    @Test
    public void TableTest1() {
        int size = 100;
        int numCols =2;
        IColumn[] columns = new IColumn[numCols];
        columns[0] = generateIntArray(size);
        columns[1] = generateDoubleArray(size);
        Schema mySchema = new Schema();
        for (int i=0; i< numCols; i++) {
            mySchema.append(columns[i].getDescription());
        }
        final FullMembership full = new FullMembership(size);
        final PartialMembershipDense partial = new PartialMembershipDense(full, row -> (row % 2) == 0);
        Table myTable = new Table(mySchema, columns, partial);
        /*myTable.printStats();*/
        final HashSubSchema filter = new HashSubSchema();
        filter.add(columns[1].getDescription().name);
        Table smallTable = myTable.compress(filter);
        /*smallTable.printStats();*/
    }
}
