package org.hillview.table;

import org.hillview.table.api.IStringConverter;
import java.util.HashMap;

/**
 * A string converter which uses an explicit hash table to map strings to integers.
 */
public abstract class BaseExplicitConverter implements IStringConverter {
    final HashMap<String, Integer> stringValue;

    BaseExplicitConverter() {
        this.stringValue = new HashMap<String, Integer>();
    }

    public void set(final String s, final int value) {
        this.stringValue.put(s, value);
    }
}
