package org.hillview.dataset.api;

import java.io.Serializable;

/**
 * Represents a type used to bring up empty IDataSets.
 */
public class Empty implements Serializable {
    private static final Empty instance = new Empty();

    public static Empty getInstance() {
        return instance;
    }
}
