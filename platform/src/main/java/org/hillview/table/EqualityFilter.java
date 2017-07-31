package org.hillview.table;

import com.sun.tools.javac.util.Assert;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This filter maps a given Table to a Table that only contains the given value in the specified column.
 */
public class EqualityFilter implements TableFilter {
    private final String columnName;
    private final Object compareValue;
    @Nullable
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
                Assert.check(compareValue instanceof String);
                break;
            case Integer:
                Assert.check(compareValue instanceof Integer);
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
                 return Objects.equals(column.getString(rowIndex), this.compareValue);
        }
    }

}
