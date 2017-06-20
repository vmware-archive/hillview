package org.hillview.table;

import org.hillview.table.api.ITable;

/**
 * A TableFilter which returns always false.
 */
public class FalseTableFilter implements TableFilter {
    @Override
    public void setTable(final ITable unused) {}

    @Override
    public boolean test(final int rowIndex) {
        return false;
    }
}
