package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.CategoryArrayColumn;
import org.hillview.table.CategoryListColumn;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;

import java.util.ArrayList;
import java.util.List;

/**
 * This map receives a column name of the input table, and returns a table with the same column, with additionally
 * that specified column converted to a categorical column.
 */
public class ToCatMap implements IMap<ITable, ITable> {
    private final String inputColName;
    private final String catColName;

    /**
     * @param inputColName The name of the column that has to be converted to a categorical column.
     */
    public ToCatMap(String inputColName) {
        this.inputColName = inputColName;
        this.catColName = inputColName + " (Cat.)";
    }

    @Override
    public ITable apply(ITable table){
        List<IColumn> columns = new ArrayList<IColumn>();

        // Copy all existing columns to new table
        Iterable<IColumn> originalColumns = table.getColumns();
        int index = -1, i = 0;
        for (IColumn inputColumn : originalColumns) {
            if (inputColumn.getName().equals(this.inputColName))
                index = i;
            columns.add(inputColumn);
            i++;
        }

        // Get the column we want to map to a categorical column
        IColumn inputColumn = table.getColumn(this.inputColName);
        if (inputColumn.getDescription().kind == ContentsKind.Category)
            throw new IllegalArgumentException("Input column " + this.inputColName + " was already categorical.");

        ColumnDescription catColumnDesc = new ColumnDescription(this.catColName, ContentsKind.Category, true);
        CategoryArrayColumn catColumn = new CategoryArrayColumn(catColumnDesc, inputColumn.sizeInRows());

        // Add the string values as categorical values in the new column
        IRowIterator rowIt = table.getMembershipSet().getIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            catColumn.set(row, inputColumn.asString(row));
            row = rowIt.getNextRow();
        }

        // Add the new column next to the input column.
        columns.add(index + 1, catColumn);

        return new Table(columns);
    }
}
