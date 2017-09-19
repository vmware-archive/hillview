package org.hillview.table.api;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Interface implemented by a column where data can be appended.
 * Columns are mutable only while being read, afterwards this interface is
 * never used.
 */
public interface IAppendableColumn extends IColumn {
    void append(@Nullable final Object value);
    default void append(@Nullable final String value)
    { this.append((Object)value); }
    default void append(final int value)
    { this.append((Object)value); }
    default void append(final double value)
    { this.append((Object)value); }
    default void append(@Nullable final LocalDateTime value)
    { this.append((Object)value); }
    default void append(@Nullable final Duration value)
    { this.append((Object)value); }
    void appendMissing();
}
