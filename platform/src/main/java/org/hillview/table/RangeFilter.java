package org.hillview.table;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.io.Serializable;


public class RangeFilter implements TableFilter, Serializable {
    final ColumnAndRange args;
    @Nullable
    IColumn column;  // not really nullable, but set later.
    @Nullable
    final IStringConverter converter;

    public RangeFilter(ColumnAndRange args) {
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
        if (Converters.checkNull(this.column).isMissing(rowIndex))
            return false;
        double d = this.column.asDouble(rowIndex, this.converter);
        return this.args.min <= d && d <= this.args.max;
    }
}
