package org.hillview.table;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

public class EqualityFilter implements TableFilter {
    String columnName;
    String compareValue;
    private IColumn column;

    public EqualityFilter(String columnName, String value) {
        this.columnName = columnName;
        this.compareValue = value;
    }

    @Override
    public void setTable(ITable table) {
        this.column = table.getColumn(this.columnName);
        // TODO: Check the type of the column, and assert that it corresponds to this.value.

    }

    /**
     * @return Whether the value at the specified row index is equal to the compare value.
     */
    @Override
    public boolean test(int rowIndex) {
        if (Converters.checkNull(this.column).isMissing(rowIndex))
            return false;
        return column.getString(rowIndex) == this.compareValue;
    }

}
