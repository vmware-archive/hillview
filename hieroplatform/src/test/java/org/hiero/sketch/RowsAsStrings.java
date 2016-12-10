package org.hiero.sketch;

import org.hiero.sketch.table.api.IColumn;

import java.util.ArrayList;

public class RowsAsStrings {
    private final ArrayList<IColumn> cols;

    public RowsAsStrings(final ArrayList<IColumn> cols){
        this.cols = cols;
    }

    public String getRow(final Integer rowIndex){
        String row = "";
        for(final IColumn thisCol: this.cols){
            row += (thisCol.asString(rowIndex) == null)? " " : thisCol.asString(rowIndex);
            row += " ";
        }
        return row;
    }
}
