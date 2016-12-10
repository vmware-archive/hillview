package org.hiero.sketch.spreadsheet;

import java.io.Serializable;

public class OrderedColumn implements Serializable {
    public final String colName;
    public final boolean isAscending;

    public OrderedColumn(final String colName, final boolean isAscending) {
        this.colName = colName;
        this.isAscending = isAscending;
    }
}
