package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IStringConverter;

import java.util.HashMap;

public abstract class BaseExplicitConverter implements IStringConverter {
    protected final HashMap<String, Integer> stringValue;

    public BaseExplicitConverter() {
        this.stringValue = new HashMap<String, Integer>();
    }

    public void set(final String s, final int value) {
        this.stringValue.put(s, value);
    }
}
