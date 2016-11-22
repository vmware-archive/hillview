package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.api.IColumn;
import java.io.Serializable;

public class QuantileList implements Serializable {

    public QuantileList(final int tableSize, final int dataSize) {
        int tableSize1 = tableSize;
        int dataSize1 = dataSize;
        IColumn[] columns = new IColumn[tableSize];
        int[] lowerBound = new int[tableSize];
        int[] upperBound = new int[tableSize];
    }
}
