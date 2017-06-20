package org.hillview.table;

import org.hillview.table.api.ITable;

/**
 * Created by lsuresh on 6/16/17.
 */
public class FalseTableFilter implements TableFilter {
    @Override
    public void setTable(final ITable table) {
        System.out.println("FalseTableFilter.setTable");
    }

    @Override
    public boolean test(final int rowIndex) {
        System.out.println("FalseTableFilter.test");
        return false;
    }
}
