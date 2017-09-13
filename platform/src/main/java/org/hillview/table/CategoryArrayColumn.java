package org.hillview.table;

import org.hillview.table.api.*;

import javax.annotation.Nullable;
import java.util.function.Consumer;

public class CategoryArrayColumn extends BaseArrayColumn
        implements IStringColumn, IMutableColumn, ICategoryColumn {
    private final int[] data;
    private final CategoryEncoding encoding;

    public CategoryArrayColumn(ColumnDescription description, final int size) {
        super(description, size);
        this.checkKind(ContentsKind.Category);
        this.encoding = new CategoryEncoding();
        this.data = new int[size];
    }

    public CategoryArrayColumn(ColumnDescription description, String[] values) {
        super(description, values.length);
        this.checkKind(ContentsKind.Category);
        this.encoding = new CategoryEncoding();
        this.data = new int[values.length];

        int i = 0;
        for (String value : values) {
            this.set(i, value);
            i++;
        }
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getString(rowIndex) == null;
    }

    @Override
    public void set(int rowIndex, @Nullable Object value) {
        if (value == null || value instanceof String)
            this.set(rowIndex, (String)value);
        else
            throw new UnsupportedOperationException("Wrong value type");
    }

    @Override
    public void set(int rowIndex, @Nullable String value) {
        this.data[rowIndex] = this.encoding.encode(value);
    }

    @Nullable
    @Override
    public Object getObject(int rowIndex) {
        return this.encoding.decode(this.data[rowIndex]);
    }

    @Override
    public String getString(int rowIndex) {
        return this.encoding.decode(this.data[rowIndex]);
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public void allDistinctStrings(Consumer<String> action) {
        this.encoding.allDistinctStrings(action);
    }
}
