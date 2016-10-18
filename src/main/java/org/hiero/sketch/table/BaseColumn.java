package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IColumn;

import java.time.Duration;
import java.util.Date;

/**
 * Base class for all columns.
 */
abstract class BaseColumn implements IColumn {
    final ColumnDescription description;

    BaseColumn(ColumnDescription description) {
        this.description = description;
    }

    public ColumnDescription getDescription() {
        return this.description;
    }

    public double getDouble(int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(int rowIndex) {
        throw new UnsupportedOperationException();
    }

    public int getInt(int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Duration getDuration(int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int rowIndex) {
        throw new UnsupportedOperationException();
    }

    public boolean isMissing(int rowIndex) { throw new UnsupportedOperationException(); }
}
