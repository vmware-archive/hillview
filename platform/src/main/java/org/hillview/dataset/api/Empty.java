package org.hillview.dataset.api;

/**
 * Represents a type used to bring up empty IDataSets.
 */
public class Empty {
    private static final Empty instance = new Empty();

    public static Empty getInstance() {
        return instance;
    }
}
