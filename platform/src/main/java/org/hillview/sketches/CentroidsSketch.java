package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Base class for centroid sketches. It computes
 * Classes that extend this class should implement a method for selecting the partitions.
 */
public abstract class CentroidsSketch implements ISketch<ITable, Centroids> {
    private final List<String> columnNames;
    private final int numCentroids;
    /**
     * @param numCentroids The number of centroids that will be computed.
     * @param columnNames List of columns that define the space in which we compute the centroids.
     */
    public CentroidsSketch(int numCentroids, List<String> columnNames) {
        this.numCentroids = numCentroids;
        this.columnNames = columnNames;
    }

    /**
     * This method returns IMembershipSets that define the partitioning of the table. The list has to be of length
     * this.numCentroids, as every centroid represents one partition.
     * @param data Table that is to be partitioned.
     * @return List of IMembershipSets that are the sets of points represented by the centroids.
     */
    abstract List<IMembershipSet> partitionTable(ITable data);

    @Override
    public Centroids create(ITable data) {
        List<IMembershipSet> partitions = this.partitionTable(data);
        return new Centroids(data, partitions, columnNames);
    }

    @Override
    public Centroids zero() {
        return new Centroids(this.numCentroids, columnNames.size());
    }

    @Override
    public Centroids add(@Nullable Centroids left, @Nullable Centroids right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}
