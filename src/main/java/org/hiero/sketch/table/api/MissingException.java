package org.hiero.sketch.table.api;

/**
 * Exception signalling an illegal access to a missing item.
 */
public class MissingException extends RuntimeException {
    public MissingException(String message) {
        super(message);
    }

    public MissingException(IColumn column, int rowIndex) {
        super("Accessing missing item in " + column.getDescription().name + ", row " + rowIndex);
    }
}
