package org.hillview.sketches;

import org.hillview.table.EqualityFilter;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;

import java.util.ArrayList;
import java.util.List;

/**
 * For every unique category in the given column name, this sketch computes the centroid of all rows belonging to
 * that category. The centroids are defined in the nD space that is defined by the columns that the given list of
 * column names specifies.
 */
public class CategoryCentroidsSketch extends CentroidsSketch {
    private List<String> categories;
    private String catColumnName;

    /**
     * @param categories The list of categories. This has to be known in advance, as the table has to be partitioned
     *                   in the same way on all compute nodes.
     * @param catColumnName The name of the categorical column where we partition by.
     * @param columnNames The names of the columns that define the nD space where the centroids are computed.
     */
    public CategoryCentroidsSketch(List<String> categories, String catColumnName, List<String> columnNames) {
        super(categories.size(), columnNames);
        this.categories = categories;
        this.catColumnName = catColumnName;
    }

    /**
     * @param data Table that is to be partitioned.
     * @return A list of IMembershipSets. Every IMembershipSet is a filtering of the table into a unique category.
     */
    @Override
    List<IMembershipSet> partitionTable(ITable data) {
        List<IMembershipSet> partitions = new ArrayList<IMembershipSet>();
        for (String category : categories) {
            EqualityFilter filter = new EqualityFilter(this.catColumnName, category);
            filter.setTable(data);
            IMembershipSet partition = data.getMembershipSet().filter(filter::test);
            partitions.add(partition);
        }
        return partitions;
    }
}
