package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.ColumnDescription;

import java.io.Serializable;

public class ColumnOrientation implements Serializable {
    public final ColumnDescription columnDescription;
    public final boolean isAscending;

    public ColumnOrientation(final ColumnDescription colDesc,
                             final boolean isAscending) {
        this.columnDescription = colDesc;
        this.isAscending = isAscending;
    }
}
