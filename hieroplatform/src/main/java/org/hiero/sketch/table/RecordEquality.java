package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ISubSchema;
import org.hiero.sketch.table.api.ITable;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that compares two IRows based on a list of columns.
 */
public class RecordEquality {
    private final List<ColumnDescription> colNames;

    public RecordEquality() {
        this.colNames = new ArrayList<ColumnDescription>();
    }

    public RecordEquality(List<ColumnDescription> cdl) { this.colNames = cdl; }

    public void append(ColumnDescription colDesc) {
        this.colNames.add(colDesc);
    }

    /**
     * Return a Schema describing the columns used to check Equality.
     */
    public Schema toSchema() {
        Schema newSchema = new Schema();
        this.colNames.forEach(newSchema::append);
        return newSchema;
    }

    /**
     * Return a subSchema containing the names of the columns used.
     */
    public ISubSchema toSubSchema() {
        final HashSubSchema subSchema = new HashSubSchema();
        for (final ColumnDescription colDesc: this.colNames) {
            subSchema.add(colDesc.name);
        }
        return subSchema;
    }

    /**
     * Test equality of a row from one table with a row in another table.
     * @param leftTable The first table
     * @param leftRow The row in the first table to be compared
     * @param rightTable The second table
     * @param rightRow The row in the second table to be compared
     * @return 1 if the rows are unequal, 0 is they are equal.
     */
    public int compareRows(ITable leftTable, int leftRow, ITable rightTable, int rightRow) {
        VirtualRowSnapshot lvrs = new VirtualRowSnapshot(leftTable, leftRow, this.toSchema());
        VirtualRowSnapshot rvrs = new VirtualRowSnapshot(rightTable, rightRow, this.toSchema());
        return (lvrs.equals(rvrs) ? 0 : 1);
    }

    public int compareRowsInTable(ITable table, int i, int j) {
        return compareRows(table, i, table, j);
    }
}
