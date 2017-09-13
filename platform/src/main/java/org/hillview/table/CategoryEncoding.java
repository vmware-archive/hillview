package org.hillview.table;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.function.Consumer;

/**
 * This class is used to compress categorical data.
 */
public class CategoryEncoding {
    // Map categorical value to a small integer
    private final HashMap<String, Integer> encoding;
    // Decode small integer into categorical value
    private final HashMap<Integer, String> decoding;

    CategoryEncoding() {
        this.encoding = new HashMap<String, Integer>(100);
        this.decoding = new HashMap<Integer, String>(100);
    }

    @Nullable
    String decode(int code) {
        if (this.decoding.containsKey(code))
            return this.decoding.get(code);
        return null;
    }

    int encode(@Nullable String value) {
        if (this.encoding.containsKey(value))
            return this.encoding.get(value);
        int encoding = this.encoding.size();
        this.encoding.put(value, encoding);
        this.decoding.put(encoding, value);
        return encoding;
    }

    public void allDistinctStrings(Consumer<String> action) {
        this.encoding.keySet().forEach(action);
    }
}
