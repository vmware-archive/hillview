package org.hillview.dataset.api;

/**
 * A Sketch result for a TableSketch that can be incrementally updated.
 */
public abstract class TableRowSketchResult implements ISketchResult {
    /**
     * Update the result by adding a contribution of the row with the specified index.
     * @param rowIndex  Index of a table row.
     */
    public abstract void add(int rowIndex);
}
