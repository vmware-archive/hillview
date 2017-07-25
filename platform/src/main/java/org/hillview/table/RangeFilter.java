package org.hillview.table;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.io.Serializable;


public class RangeFilter implements TableFilter, Serializable {
    final FilterDescription args;
    @Nullable
    IColumn column;  // not really nullable, but set later.
    @Nullable
    final IStringConverter converter;

    public RangeFilter(FilterDescription args) {
        this.args = args;
        this.column = null;
        if (args.bucketBoundaries != null)
            this.converter = new SortedStringsConverter(
                    args.bucketBoundaries, (int)Math.ceil(args.min), (int)Math.floor(args.max));
        else
            this.converter = null;
    }

    @Override
    public void setTable(ITable table) {
        IColumn col = table.getColumn(this.args.columnName);
        this.column = Converters.checkNull(col);
    }

    public boolean test(int rowIndex) {
        boolean result;
        if (Converters.checkNull(this.column).isMissing(rowIndex))
            result = false;
        else {
            double d = this.column.asDouble(rowIndex, this.converter);
            result = this.args.min <= d && d <= this.args.max;
        }
        if (this.args.complement)
            result = !result;
        return result;
    }
}
