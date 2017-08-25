package org.hillview.table.api;

import java.time.Duration;
import java.time.LocalDateTime;

public interface IMutableColumn extends IColumn {
    void set(final int rowIndex, final Object value);
    default void set(final int rowIndex, final String value) { this.set(rowIndex, (Object)value); }
    default void set(final int rowIndex, final int value) { this.set(rowIndex, (Object)value); }
    default void set(final int rowIndex, final double value) { this.set(rowIndex, (Object)value); }
    default void set(final int rowIndex, final LocalDateTime value) { this.set(rowIndex, (Object)value); }
    default void set(final int rowIndex, final Duration value) { this.set(rowIndex, (Object)value); }
    void setMissing(final int rowIndex);
}
