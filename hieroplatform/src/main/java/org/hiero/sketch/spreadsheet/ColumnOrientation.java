package org.hiero.sketch.spreadsheet;

import java.io.Serializable;

public class ColumnOrientation implements Serializable {
    public final String colName;
    public final boolean isAscending;

    public ColumnOrientation(final String colName, final boolean isAscending) {
        this.colName = colName;
        this.isAscending = isAscending;
    }
}
