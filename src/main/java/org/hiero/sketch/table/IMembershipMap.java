package org.hiero.sketch.table;

/**
 * A IMembershipMap is a representation of a set of integers.
 * These integers represent row indexes in a table.  If an integer
 * is in a IMembershipMap, then it is present in the table.
 */
public interface IMembershipMap {
    boolean isMember(int rowIndex);
}
