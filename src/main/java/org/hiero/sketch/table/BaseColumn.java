package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IColumn;

import java.time.Duration;
import java.util.Date;

/**
 * Base class for all columns.
 */
abstract class BaseColumn implements IColumn {
    final ColumnDescription description;

    BaseColumn(final ColumnDescription description) {
        this.description = description;
    }

    public ColumnDescription getDescription() {
        return this.description;
    }

    public double getDouble(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    public int getInt(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    public boolean isMissing(final int rowIndex) { throw new UnsupportedOperationException(); }
}
