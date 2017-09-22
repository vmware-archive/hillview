package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ICategoryColumn;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;

/**
 * For each of the specified column names it computes the set of
 * all unique strings in the column.  It assumes that there are few
 * distinct strings in each such column; this makes sense when columns
 * are categorical.
 * This test is special, because it ignores the table membership set:
 * it just gets all strings in the given columns.
 */
public class DistinctStringsSketch implements ISketch<ITable, JsonList<DistinctStrings>> {
    private final int maxSize;
    private final String[] colNames;

    public DistinctStringsSketch(int maxSize, String[] colNames) {
        this.maxSize = maxSize;
        this.colNames = colNames;
    }

    @Override
    public JsonList<DistinctStrings> zero() {
        JsonList<DistinctStrings> result = new JsonList<DistinctStrings>(this.colNames.length);
        for (String colName : this.colNames)
            result.add(new DistinctStrings(this.maxSize));
        return result;
    }

    @Override
    public JsonList<DistinctStrings> add(
            @Nullable JsonList<DistinctStrings> left,
            @Nullable JsonList<DistinctStrings> right) {
        JsonList<DistinctStrings> result = new JsonList<DistinctStrings>(this.colNames.length);
        for (int i = 0; i < this.colNames.length; i++)
            result.add(Converters.checkNull(left).get(i).union(Converters.checkNull(right).get(i)));
        return result;
    }

    @Override
    public JsonList<DistinctStrings> create(final ITable data) {
        JsonList<DistinctStrings> result = this.getZero();
        for (int i = 0; i < this.colNames.length; i++) {
            IColumn col = data.getColumn(this.colNames[i]);
            final DistinctStrings ri = result.get(i);
            ri.setColumnSize(col.sizeInRows());
            if (col instanceof ICategoryColumn) {
                ICategoryColumn cc = (ICategoryColumn)col;
                cc.allDistinctStrings(ri::add);
            } else {
                for (int row = 0; row < col.sizeInRows(); row++) {
                    String s = col.getString(row);
                    ri.add(s);
                }
            }
        }
        return result;
    }
}
