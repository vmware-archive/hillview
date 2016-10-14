package org.hiero.sketch.table;

import java.util.HashMap;

/**
 * A string converter which uses an explicit hash table to map strings to integers.
 */
public final class ExplicitStringConverter implements IStringConverter {
    private HashMap<String, Integer> stringValue;

    public ExplicitStringConverter() {
        this.stringValue = new HashMap<String, Integer>();
    }

    /* Will throw an exception when String is not known */
    @Override
    public double asDouble(String string) {
        int val = this.stringValue.get(string);
        return val;
    }

    public void set(String s, int value) {
        this.stringValue.put(s, value);
    }
}
