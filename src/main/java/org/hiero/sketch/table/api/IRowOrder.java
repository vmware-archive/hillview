package org.hiero.sketch.table.api;

public interface IRowOrder {
    /**
     * @return Total number of rows.
     */
    int getSize();

    /**
     * @return An iterator over all the rows in the membership map.
     * The iterator is initialized to point at the "first" row.
     */
    IRowIterator getIterator();
}
