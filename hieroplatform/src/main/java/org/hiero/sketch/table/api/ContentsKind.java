package org.hiero.sketch.table.api;

import java.io.Serializable;

/**
 * Describes the kind of data that is in the column,
 */
public enum ContentsKind implements Serializable {
    String,
    Date,  /* java.Util.Date values */
    Int,
    Json,
    Double,
    Duration; /* java.time.Duration values */

    /**
     * True if this kind of information requires a Java Object for storage.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isObject() {
        switch (this) {
            case String:
            case Date:
            case Json:
            case Duration:
                return true;
            case Int:
            case Double:
            default:
                return false;
        }
    }
}
