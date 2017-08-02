package org.hillview.table;

import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This filter maps a given Table to a Table that only contains the given value in the specified column.
 */
public class EqualityFilter implements TableFilter {
    private final String columnName;
    private final Object compareValue;
    private final boolean complement;
    @Nullable
    private IColumn column;
    @Nullable
    private ContentsKind compareKind;

    /**
     * Make a filter that accepts rows that (do not) have a specified value in the specified column.
     * @param columnName Name of the column that is compared.
     * @param value Value that is compared for (in)equality in the column.
     * @param complement If true, invert the filter such that it checks for inequality.
     */
    public EqualityFilter(String columnName, Object value, boolean complement) {
        this.columnName = columnName;
        this.compareValue = value;
        this.complement = complement;
    }

    /**
     * Make a filter that accepts rows that have a specified value in the specified column.
     * @param columnName Name of the column that is compared.
     * @param value Value that is compared for equality in the column.
     */
    public EqualityFilter(String columnName, Object value) {
        this(columnName, value, false);
    }

    @Override
    public void setTable(ITable table) {
        this.column = table.getColumn(this.columnName);
        this.compareKind = column.getDescription().kind;

        // Check the types. Just Strings and Integers for now.
        switch (this.compareKind) {
            case Category:
            case String:
            case Json:
                Assert.assertTrue(compareValue instanceof String);
                break;
            case Integer:
                Assert.assertTrue(compareValue instanceof Integer);
                break;
        }
    }

    /**
     * @return Whether the value at the specified row index is equal to the compare value.
     */
    @Override
    public boolean test(int rowIndex) {
        IColumn column = Converters.checkNull(this.column);
        ContentsKind compareKind = Converters.checkNull(this.compareKind);

        boolean result;
        if (column.isMissing(rowIndex)) {
            result = false;
        } else {
            switch (compareKind) {
                case Integer:
                    result = column.getInt(rowIndex) == (Integer) this.compareValue;
                    break;
                case Category:
                case String:
                case Json:
                default:
                    result = Objects.equals(column.getString(rowIndex), this.compareValue);
            }
        }

        return this.complement != result;
    }
}
