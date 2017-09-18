package org.hillview.sketches;

import java.util.List;

/**
 * For every unique category in the given column name, this sketch computes the centroid of all rows belonging to
 * that category. The centroids are defined in the nD space that is defined by the columns that the given list of
 * column names specifies.
 */
public class CategoryCentroidsSketch extends CentroidsSketch<String> {
    /**
     * @param catColumnName The name of the categorical column where we partition by.
     * @param columnNames The names of the columns that define the nD space where the centroids are computed.
     */
    public CategoryCentroidsSketch(String catColumnName, List<String> columnNames) {
        super((table, row) -> table.getColumn(catColumnName).asString(row), columnNames);
    }
}
