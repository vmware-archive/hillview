package org.hillview.table;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nonnull;


/**
 * This filter maps a given Table to a Table that only contains the given value in the specified column.
 */
public class EqualityFilter implements TableFilter {
    @Nonnull
    private String columnName;
    private Object compareValue;
    private IColumn column;

    public EqualityFilter(String columnName, Object value) {
        this.columnName = columnName;
        this.compareValue = value;
    }

    @Override
    public void setTable(ITable table) {
        this.column = table.getColumn(this.columnName);

        // Check the types. Just Strings and Integers for now.
        switch (column.getDescription().kind) {
            case Category:
                assert(compareValue instanceof String);
                break;
            case Integer:
                assert(compareValue instanceof Integer);
                break;
        }
    }

    /**
     * @return Whether the value at the specified row index is equal to the compare value.
     */
    @Override
    public boolean test(int rowIndex) {
        if (Converters.checkNull(this.column).isMissing(rowIndex))
            return false;
        switch (column.getDescription().kind) {
            case Integer:
                return column.getInt(rowIndex) == (Integer) this.compareValue;
            case Category:
            default:
                 return column.getString(rowIndex).equals(this.compareValue);
        }
    }

}
