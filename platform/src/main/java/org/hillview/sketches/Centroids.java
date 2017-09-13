package org.hillview.sketches;

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.utils.BlasConversions;
import org.jblas.DoubleMatrix;
import org.jblas.ranges.AllRange;
import org.jblas.ranges.PointRange;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This class represents a set of centroids of a partitioning of a table. The number of centroids should not be too
 * large, as they are shipped over the network.
 */
public class Centroids implements IJson {
    /**
     * Encodes the position of the centroids. Every row is a centroid, and the columns are the dimensions.
     * Each partition has a centroid, which is simply the mean of the nD points in the partition.
     * Should be computed with computeCentroids() by the head node.
     */
    @Nullable
    public DoubleMatrix centroids;
    /**
     * Holds the sum of the values in the partitions. Every row is the sum of all rows belonging to the corresponding
     * partition.
     */
    public DoubleMatrix sums;
    /**
     * Registers how many rows are represented by every centroid.
     */
    public long[] counts;

    /**
     * Constructs a zero-centroids. Needs to know the dimensions.
     * @param numPartitions Number of partitions.
     * @param numColumns Number of columns (= number of dimensions in the centroid space).
     */
    public Centroids(int numPartitions, int numColumns) {
        this.counts = new long[numPartitions];
        this.sums = DoubleMatrix.zeros(numPartitions, numColumns);
    }

    /**
     * Construct a centroids object that has processed the sums of the partitions in 'table'.
     * The final centroids are not computed yet, this should be done after the sketch finishes.
     * @param table The ITable that holds the data from which to compute the centroids.
     * @param partitions The list of IMembershipSets that define the partitions.
     * @param columnNames The list of column names of dimensions that define the nD space.
     */
    public Centroids(ITable table, List<IMembershipSet> partitions, List<String> columnNames) {
        this.counts = new long[partitions.size()];
        this.sums = DoubleMatrix.zeros(partitions.size(), columnNames.size());

        int i = 0;
        for (IMembershipSet partition : partitions) {
            // The count is simply the number of rows that the sum represents.
            this.counts[i] = partition.getSize();
            if (this.counts[i] > 0) {
                // Consider only part of the data that is in the partition.
                ITable partitionTable = table.selectRowsFromFullTable(partition);
                // Get the matrix, compute the sum of every column, and put it in the sums matrix.
                DoubleMatrix tableData = BlasConversions.toDoubleMatrix(partitionTable, columnNames);
                this.sums.put(new PointRange(i), new AllRange(), tableData.columnSums());
            }
            i++;
        }
    }

    /**
     * Compute the aggregate of two centroid objects. This simply sums the sums and counts.
     * @param other Object that holds information for the other set of centroids.
     * @return Centroid object that represents the aggregate of the rows that both objects represent.
     */
    public Centroids union(Centroids other) {
        this.sums.addi(other.sums);
        for (int i = 0; i < this.sums.rows; i++) {
            this.counts[i] += other.counts[i];
        }
        return this;
    }

    /**
     * Computes the centroids, based on the information in the 'sums' and 'counts' fields.
     * This only has to be done once: when the sketch has finished.
     * Sets the centroids in the objects 'centroids' field.
     * @return The computed centroids.
     */
    public DoubleMatrix computeCentroids() {
        this.centroids = DoubleMatrix.zeros(this.sums.rows, this.sums.columns);
        for (int i = 0; i < this.counts.length; i++)
            if (this.counts[i] > 0)
                this.centroids.put(
                        new PointRange(i), new AllRange(),
                        this.sums.get(new PointRange(i), new AllRange()).div(this.counts[i])
                );
        return this.centroids;
    }
}
