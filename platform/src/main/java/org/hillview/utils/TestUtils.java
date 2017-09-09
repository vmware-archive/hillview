package org.hillview.utils;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;

public class TestUtils {
    /**
     * Prints a table to stdout.
     * @param table The table that is printed.
     */
    public static void printTable(ITable table) {
        StringBuilder header = new StringBuilder("| ");
        for (IColumn col : table.getColumns()) {
            header.append(col.getName()).append("\t| ");
        }
        header.append("\t|");
        System.out.println(header);
        IRowIterator rowIt = table.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            StringBuilder rowString = new StringBuilder("| ");
            for (IColumn col : table.getColumns()) {
                rowString.append(col.asString(row)).append("\t| ");
            }
            rowString.append("\t");
            System.out.println(rowString);
            row = rowIt.getNextRow();
        }
    }
}
