package org.hillview.sketches;

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.utils.BlasConversions;
import org.jblas.DoubleMatrix;
import org.jblas.ranges.AllRange;
import org.jblas.ranges.PointRange;

import java.util.List;

/**
 * This class represents a set of centroids of a partitioning of a table. The number of centroids should not be too
 * large, as they are shipped over the network.
 */
public class Centroids implements IJson {
    /**
     * Encodes the position of the centroids. Every row is a centroid, and the columns are the dimensions.
     * Each partition has a centroid, which is simply the mean of the nD points in the partition.
     */
    public DoubleMatrix centroids;
    /**
     * Registers how many rows are represented by every centroid.
     */
    public long[] support;
    /**
     * Constructs a zero-centroids. Needs to know the dimensions.
     * @param numPartitions Number of partitions.
     * @param numColumns Number of columns (= number of dimensions in the centroid space).
     */
    public Centroids(int numPartitions, int numColumns) {
        this.support = new long[numPartitions];
        this.centroids = DoubleMatrix.zeros(numPartitions, numColumns);
    }

    /**
     * Construct a centroids object that holds the centroids of the partitions in 'table'.
     * @param table The ITable that holds the data from which to compute the centroids.
     * @param partitions The list of IMembershipSets that define the partitions.
     * @param columnNames The list of column names of dimensions that define the nD space.
     */
    public Centroids(ITable table, List<IMembershipSet> partitions, List<String> columnNames) {
        this.support = new long[partitions.size()];
        this.centroids = new DoubleMatrix(partitions.size(), columnNames.size());

        int i = 0;
        for (IMembershipSet partition : partitions) {
            // Consider only part of the data that is in the partition.
            ITable partitionTable = table.selectRowsFromFullTable(partition);
            // Get the matrix and compute the centroid as the mean
            DoubleMatrix tableData = BlasConversions.toDoubleMatrix(partitionTable, columnNames);
            DoubleMatrix centroid = tableData.columnMeans();
            centroids.put(new PointRange(i), new AllRange(), centroid);
            // The support is simply the number of rows that the centroid represents.
            this.support[i] = partition.getSize();
            i++;
        }
    }

    /**
     * Compute the weighted sum of two sets of centroids.
     * @param other Object that holds the other set of centroids.
     * @return Centroid object that represents the aggregate of the rows that both objects represent.
     */
    public Centroids union(Centroids other) {
        for (int i = 0; i < this.centroids.rows; i++) {
            DoubleMatrix newCentroid;
            if (this.support[i] + other.support[i] == 0) {
                newCentroid = DoubleMatrix.zeros(1, this.centroids.columns);
            } else {
                DoubleMatrix myCentroid = this.centroids.get(new PointRange(i), new AllRange());
                DoubleMatrix otherCentroid = other.centroids.get(new PointRange(i), new AllRange());
                // Compute weights by using the support for each partition.
                float myWeight = this.support[i] / (this.support[i] + other.support[i]);
                float otherWeight = other.support[i] / (this.support[i] + other.support[i]);
                // New centroid is the weighted sum of the two.
                newCentroid = myCentroid.mul(myWeight).add(otherCentroid.mul(otherWeight));
            }
            this.centroids.put(new PointRange(i), new AllRange(), newCentroid);
            this.support[i] += other.support[i];
        }
        return this;
    }
}
