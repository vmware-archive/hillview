package org.hiero.sketch.table.api;

/**
 * Subset of the columns in a schema.
 */
public interface ISubSchema {
    boolean isColumnPresent(String name);
}
