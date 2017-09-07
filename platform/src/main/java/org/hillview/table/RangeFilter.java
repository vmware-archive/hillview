package org.hillview.table;

import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.io.Serializable;


public class RangeFilter implements TableFilter, Serializable {
    final RangeFilterDescription args;
    @Nullable
    ColumnAndConverter column;  // not really nullable, but set later.

    public RangeFilter(RangeFilterDescription args) {
        this.args = args;
        this.column = null;
    }

    @Override
    public void setTable(ITable table) {
        IStringConverter converter = null;
        if (args.bucketBoundaries != null)
            converter = new SortedStringsConverter(
                    args.bucketBoundaries, (int)Math.ceil(args.min), (int)Math.floor(args.max));
        this.column = new ColumnAndConverter(table.getColumn(this.args.columnName), converter);
    }

    public boolean test(int rowIndex) {
        boolean result;
        if (Converters.checkNull(this.column).isMissing(rowIndex))
            result = false;
        else {
            double d = this.column.asDouble(rowIndex);
            result = this.args.min <= d && d <= this.args.max;
        }
        if (this.args.complement)
            result = !result;
        return result;
    }
}
