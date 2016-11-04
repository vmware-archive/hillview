package org.hiero.sketch.table.api;

import org.hiero.sketch.table.ColumnDescription;

import java.time.Duration;
import java.util.Date;

/**
 * Interface describing operations on a column.
 * A column is just a big vector of values.
 */
public interface IColumn {
    ColumnDescription getDescription();

    String getString(int rowIndex);
    double getDouble(int rowIndex);
    Date getDate(int rowIndex);
    int getInt(int rowIndex);
    Duration getDuration(int rowIndex);

    /**
     * @param rowIndex Row to check
     * @return True if the data in the specified row is missing.
     */
    boolean isMissing(int rowIndex);

    /**
     * @return Number of rows in the column.
     */
    int sizeInRows();

    /**
     * Whatever the internal data type, return a double.
     */
    double asDouble(int rowIndex, IStringConverter converter);

    String asString(int rowIndex);

    RowComparator getComparator();
}
