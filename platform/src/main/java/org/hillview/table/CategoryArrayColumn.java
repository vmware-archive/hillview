package org.hillview.table;

import org.hillview.table.api.*;

import javax.annotation.Nullable;
import java.util.HashMap;

public class CategoryArrayColumn extends BaseArrayColumn implements IStringColumn, IMutableColumn {

    private final HashMap<String, Integer> encoding;
    private final HashMap<Integer, String> decoding;
    private final int[] data;

    public CategoryArrayColumn(ColumnDescription description, final int size) {
        super(description, size);
        this.encoding = new HashMap<String, Integer>(100);
        this.decoding = new HashMap<Integer, String>(100);
        this.data = new int[size];
    }

    @Nullable
    String decode(int code) {
        if (this.decoding.containsKey(code))
            return this.decoding.get(code);
        return null;
    }

    int encode(String value) {
        if (this.encoding.containsKey(value))
            return this.encoding.get(value);
        int encoding = this.encoding.size();
        this.encoding.put(value, encoding);
        this.decoding.put(encoding, value);
        return encoding;
    }

    @Override
    public void set(int rowIndex, Object value) {
        if (value instanceof String)
            this.set(rowIndex, (String) value);
        else
            throw new UnsupportedOperationException();
    }

    @Override
    public void set(int rowIndex, String value) {
        this.data[rowIndex] = this.encode(value);
    }

    @Nullable
    @Override
    public Object getObject(int rowIndex) {
        return this.decode(this.data[rowIndex]);
    }

    @Override
    public String getString(int rowIndex) {
        return this.decode(this.data[rowIndex]);
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }
}
