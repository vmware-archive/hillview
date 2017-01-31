package org.hiero.sketch.table.api;

/**
 * An iterator over the rows returns the indexes of the rows of a table.
 * It returns -1 when the iteration is completed.
 */
public interface IRowIterator {
    // Returns -1 when iteration is completed; else it returns
    // the index of the table row.
    int getNextRow();
}
