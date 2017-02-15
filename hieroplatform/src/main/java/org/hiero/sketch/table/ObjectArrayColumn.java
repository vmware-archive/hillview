package org.hiero.sketch.table;

import javax.annotation.Nonnull;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.IndexComparator;

import java.time.Duration;
import java.time.LocalDateTime;

/*
 * Column of objects of any type; only for moving data around. Size of column expected to be small.
 */
public final class ObjectArrayColumn extends BaseArrayColumn {
    @Nonnull private final Object[] data;

    public ObjectArrayColumn(@Nonnull final ColumnDescription description, final int size) {
        super(description, size);
        this.data = new Object[size];
    }

    public ObjectArrayColumn(@Nonnull final ColumnDescription description,
                             @Nonnull final Object[] data) {
        super(description, data.length);
        this.data = data;
    }

    @Override
    public int sizeInRows() { return this.data.length; }

    @Override
    public double asDouble(final int rowIndex, @Nonnull final IStringConverter converter) {
        switch (ObjectArrayColumn.this.description.kind) {
            case String:
                return converter.asDouble(this.getString(rowIndex));
            case Date:
                return converter.asDouble(this.getDate(rowIndex).toString());
            case Int:
                return converter.asDouble(String.valueOf(this.getInt(rowIndex)));
            case Json:
                return converter.asDouble(this.getString(rowIndex));
            case Double:
                return this.getDouble(rowIndex);
            case Duration:
                return converter.asDouble(this.getDuration(rowIndex).toString());
            default:
                throw new RuntimeException("Unexpected data type");
        }
    }

    @Override
    public String asString(final int rowIndex) {
        return this.data[rowIndex].toString();
    }

    @Override
    public IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = ObjectArrayColumn.this.isMissing(i);
                final boolean jMissing = ObjectArrayColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    switch (ObjectArrayColumn.this.description.kind) {
                        case String:
                            return ObjectArrayColumn.this.getString(i).compareTo(
                                    ObjectArrayColumn.this.getString(j));
                        case Date:
                            return ObjectArrayColumn.this.getDate(i).compareTo(
                                    ObjectArrayColumn.this.getDate(j));
                        case Int:
                            return Integer.compare(ObjectArrayColumn.this.getInt(i),
                                    ObjectArrayColumn.this.getInt(j));
                        case Json:
                            return ObjectArrayColumn.this.getString(i).compareTo(
                                    ObjectArrayColumn.this.getString(j));
                        case Double:
                            return Double.compare(ObjectArrayColumn.this.getDouble(i),
                                    ObjectArrayColumn.this.getDouble(j));
                        case Duration:
                            return ObjectArrayColumn.this.getDuration(i).
                                    compareTo(ObjectArrayColumn.this.getDuration(j));
                        default:
                            throw new RuntimeException("Unexpected data type");
                    }
                }
            }
        };
    }

    @Override
    public int getInt(final int rowIndex) {
        return (int)this.data[rowIndex];
    }

    @Override
    public double getDouble(final int rowIndex) {
        return (double)this.data[rowIndex];
    }

    @Override
    public LocalDateTime getDate(final int rowIndex) {
        return (LocalDateTime) this.data[rowIndex];
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        return (Duration)this.data[rowIndex];
    }
    @Override
    public String getString(final int rowIndex) {
        return (String)this.data[rowIndex];
    }

    public void set(final int rowIndex, final Object value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex) { return this.data[rowIndex] == null; }

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}
}
