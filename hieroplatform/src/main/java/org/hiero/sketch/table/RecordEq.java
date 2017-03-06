package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.ISubSchema;
import org.hiero.sketch.table.api.ITable;

import java.util.ArrayList;
import java.util.List;

public class RecordEq {
    private List<ColumnDescription> colNames;

    public RecordEq() {
        this.colNames = new ArrayList<ColumnDescription>();
    }

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

    public int compareRows(ITable leftTable, int leftRow, ITable rightTable, int rightRow) {
        int outcome = 0;
        for (ColumnDescription colDesc : this.colNames) {
            final IColumn leftCol = leftTable.getColumn(colDesc.name);
            final IColumn rightCol = rightTable.getColumn(colDesc.name);
            final boolean leftMissing = leftCol.isMissing(leftRow);
            final boolean rightMissing = rightCol.isMissing(rightRow);
            if ((leftMissing && !rightMissing) || (!leftMissing && rightMissing)) {
                return 1;
            } else {
                switch (colDesc.kind) {
                    case String:
                    case Json:
                        outcome = leftCol.getString(leftRow).
                                compareTo(rightCol.getString(rightRow));
                        break;
                    case Date:
                        outcome = leftCol.getDate(leftRow).
                                compareTo(rightCol.getDate(rightRow));
                        break;
                    case Int:
                        outcome = Integer.compare(leftCol.getInt(leftRow),
                                rightCol.getInt(rightRow));
                        break;
                    case Double:
                        outcome = Double.compare(leftCol.getDouble(leftRow),
                                rightCol.getDouble(rightRow));
                        break;
                    case Duration:
                        outcome = leftCol.getDuration(leftRow).
                                compareTo(rightCol.getDuration(rightRow));
                        break;
                }
            }
            if (outcome != 0) return 1;
        }
        return 0;
    }

    public int compareRowsInTable(ITable Table, int i, int j) {
        return compareRows(Table, i, Table, j);
    }
}
