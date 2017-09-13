package org.hillview.table;

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;

public class SparseColumn extends BaseColumn
        implements IMutableColumn, IStringColumn, IDoubleColumn, IIntColumn, IDateColumn, IDurationColumn {
    protected HashMap<Integer, Object> data;
    int size;

    public SparseColumn(ColumnDescription desc, int size) {
        super(desc);
        this.size = size;
        this.data = new HashMap<Integer, Object>();
    }

    @Override
    public int sizeInRows() {
        return this.size;
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public double asDouble(int rowIndex, @Nullable IStringConverter converter) {
        if (this.isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        switch (this.description.kind) {
            case Category:
            case String:
            case Json:
                return Converters.checkNull(converter).asDouble(this.getString(rowIndex));
            case Integer:
                return this.getInt(rowIndex);
            case Date:
                return Converters.toDouble(this.getDate(rowIndex));
            case Double:
                return this.getDouble(rowIndex);
            case Duration:
                return Converters.toDouble(this.getDuration(rowIndex));
            default:
                throw new RuntimeException("Unexpected kind " + this.description.kind);
        }
    }

    @Nullable
    @Override
    public String asString(int rowIndex) {
        Object o = this.getObject(rowIndex);
        if (o == null)
            return null;
        return o.toString();
    }

    @Override
    public IndexComparator getComparator() {
        return new IndexComparator() {
            @SuppressWarnings("ConstantConditions")
            @Override
            public int compare(Integer o1, Integer o2) {
            boolean o1m = SparseColumn.this.isMissing(o1);
            boolean o2m = SparseColumn.this.isMissing(o2);
            if (o1m) {
                if (o2m)
                    return 0;
                else
                    return 1;
            }
            if (o2m)
                return -1;

            switch (SparseColumn.this.description.kind) {
                case Category:
                case String:
                case Json:
                    return SparseColumn.this.getString(o1).compareTo(
                            SparseColumn.this.getString(o2));
                case Integer:
                    return Integer.compare(SparseColumn.this.getInt(o1),
                            SparseColumn.this.getInt(o2));
                case Date:
                case Double:
                case Duration:
                    return Double.compare(SparseColumn.this.asDouble(o1, null),
                            SparseColumn.this.asDouble(o2, null));
                default:
                    throw new RuntimeException("Unexpected kind " +
                            SparseColumn.this.description.kind);
            }
            }
        };
    }

   @Override
    @SuppressWarnings("ConstantConditions")
    public long hashCode64(int rowIndex, LongHashFunction hash) {
        if (isMissing(rowIndex))
            return MISSING_HASH_VALUE;
        switch (this.description.kind) {
            case Category:
            case String:
            case Json:
                return hash.hashChars(this.getString(rowIndex));
            case Integer:
                return hash.hashInt(this.getInt(rowIndex));
            case Date:
            case Double:
            case Duration:
                return hash.hashLong(Double.doubleToRawLongBits(this.asDouble(rowIndex, null)));
            default:
                throw new RuntimeException("Unexpected kind " + this.description.kind);
        }
    }

    @Override
    public Object getObject(final int rowIndex) {
        if (this.data.containsKey(rowIndex))
            return this.data.get(rowIndex);
        return null;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return !this.data.containsKey(rowIndex);
    }

    public void set(final int rowIndex, @Nullable final Object value) {
        if (value == null) {
            if (this.data.containsKey(rowIndex))
                this.data.remove(rowIndex);
            return;
        }
        this.data.put(rowIndex, value);
    }

    @Override
    public void setMissing(final int rowIndex) {}

    public void set(final int rowIndex, final double value) {
        this.data.put(rowIndex, value);
    }

    @Override
    public double getDouble(final int rowIndex) {
        Object o = this.getObject(rowIndex);
        if (o == null)
            throw new MissingException(this, rowIndex);
        return (double)o;
    }

    @Override
    public LocalDateTime getDate(final int rowIndex) {
        Object o = this.getObject(rowIndex);
        return (LocalDateTime)o;
    }

    @Override
    public int getInt(final int rowIndex) {
        Object o = this.getObject(rowIndex);
        if (o == null)
            throw new MissingException(this, rowIndex);
        return (int)o;
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        Object o = this.getObject(rowIndex);
        return (Duration)o;
    }

    @Override
    public String getString(final int rowIndex) {
        Object o = this.getObject(rowIndex);
        return (String)o;
    }

    @Override
    public IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        switch (this.description.kind) {
            case Category:
            case String:
            case Json:
                return IStringColumn.super.convertKind(kind, newColName, set);
            case Integer:
                return IIntColumn.super.convertKind(kind, newColName, set);
            case Date:
                return IDateColumn.super.convertKind(kind, newColName, set);
            case Double:
                return IDoubleColumn.super.convertKind(kind, newColName, set);
            case Duration:
                return IDurationColumn.super.convertKind(kind, newColName, set);
            default:
                throw new RuntimeException("Unexpected kind " + this.description.kind);
        }
    }
}
