package org.hillview.table;

import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import java.io.Serializable;

/**
 * Filter applied to two columns.
 */
public class Range2DFilter implements TableFilter, Serializable {
    final RangeFilter first;
    final RangeFilter second;

    public Range2DFilter(RangeFilterPair args) {
        this.first = new RangeFilter(Converters.checkNull(args.first));
        this.second = new RangeFilter(Converters.checkNull(args.second));
    }

    public void setTable(ITable table) {
        this.first.setTable(table);
        this.second.setTable(table);
    }

    public boolean test(int rowIndex) {
        return this.first.test(rowIndex) && this.second.test(rowIndex);
    }
}
