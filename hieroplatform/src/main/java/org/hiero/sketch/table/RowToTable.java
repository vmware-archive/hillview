package org.hiero.sketch.table;

import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.ITable;

import java.util.Iterator;

/**
 * This class lets us compare a RowSnapShot to entries from a Table according to a prescribed
 * RecordOrder. The schema of the snapshot should be a subset of that of the Table.
 * This method is used to generate the NextK items in some order starting at a prescribed row by
 * NextKSketch.
 */
public class RowToTable {
    private final RowSnapshot topRow;
    private final ITable table;
    private final RecordOrder recordOrder;

    public RowToTable(RowSnapshot topRow, ITable table, RecordOrder recordOrder ) {
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
            if (iCol.isMissing(i) && this.topRow.isMissing(colName))
                outcome = 0;
            else if (iCol.isMissing(i)) {
                outcome = 1;
            } else if (this.topRow.isMissing(colName)) {
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
        return 0;
    }
}