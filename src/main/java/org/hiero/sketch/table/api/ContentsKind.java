package org.hiero.sketch.table.api;

/**
 * Describes the kind of data that is in the column,
 */
public enum ContentsKind {
    String,
    Date,  /* java.Util.Date values */
    Int,
    Json,
    Double,
    Duration; /* java.time.Duration values */

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
