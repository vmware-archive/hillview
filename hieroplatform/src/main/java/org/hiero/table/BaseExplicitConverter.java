package org.hiero.table;

import org.hiero.table.api.IStringConverter;
import java.util.HashMap;

/**
 * A string converter which uses an explicit hash table to map strings to integers.
 */

public abstract class BaseExplicitConverter implements IStringConverter {
    protected final HashMap<String, Integer> stringValue;

    public BaseExplicitConverter() {
        this.stringValue = new HashMap<String, Integer>();
    }

    public void set(final String s, final int value) {
        this.stringValue.put(s, value);
    }
}
