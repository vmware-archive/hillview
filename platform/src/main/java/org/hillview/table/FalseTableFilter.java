package org.hillview.table;

import org.hillview.table.api.ITable;


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
