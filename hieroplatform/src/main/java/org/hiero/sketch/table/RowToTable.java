package org.hiero.sketch.table;

import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.ITable;

import java.util.Iterator;

public class RowToTable {
    private final RowSnapshot topRow;
    private final ITable table;
    private final RecordOrder recordOrder;

    public RowToTable(RowSnapshot topRow, ITable table, RecordOrder recordOrder) {
        this.topRow = topRow;
        this.table = table;
        this.recordOrder = recordOrder;
    }

    /**
     * Compares Row i of Table to topRow according to recordOrder.
     * @param i Index of a row in the Table.
     * @return 1 if row i is greater, 0 if they are equal, -1 if it is less.
     */
    public int compareToRow(int i) {
        int outcome = 0;
        Iterator<ColumnSortOrientation> it = this.recordOrder.iterator();
        while (it.hasNext()) {
            ColumnSortOrientation ordCol = it.next();
            String colName = ordCol.columnDescription.name;
            IColumn iCol = this.table.getColumn(colName);
            boolean topRowMissing = (this.topRow.get(colName) == null);
            if (iCol.isMissing(i) && topRowMissing)
                outcome = 0;
            else if (iCol.isMissing(i)) {
                outcome = 1;
            } else if (topRowMissing) {
                outcome = -1;
            } else {
                switch (this.table.getSchema().getKind(colName)) {
                    case String:
                    case Json:
                        outcome = iCol.getString(i).compareTo(this.topRow.getString(colName));
                        break;
                    case Date:
                        outcome = iCol.getDate(i).compareTo(this.topRow.getDate(colName));
                        break;
                    case Int:
                        outcome = Integer.compare(iCol.getInt(i), this.topRow.getInt(colName));
                        break;
                    case Double:
                        outcome = Double.compare(iCol.getDouble(i), this.topRow.getDouble(colName));
                        break;
                    case Duration:
                        outcome = iCol.getDuration(i).compareTo(this.topRow.getDuration(colName));
                        break;
                }
            }
            if (!ordCol.isAscending) {
                outcome *= -1;
            }
            if (outcome != 0)
                return outcome;
        }
        return outcome;
    }
}
