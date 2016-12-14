package org.hiero.sketch.spreadsheet;

/**
 * meta data for one dimensional bucket set
 */
public interface IBucketsDescription1D {

    int getNumOfBuckets();

    /**
     * @param index is a number in 0...numOfBuckets - 1
     * @return the left boundary of the bucket
     */
    double getLeftBoundary(int index);

    /**
     * @param index is a number in 0...numOfBuckets - 1
     * @return the right boundary of the bucket
     */
    double getRightBoundary(int index);

    /**
     * @param item is a double that will be placed in a bucket of the histogram
     * @return the index of the bucket in which the item should be placed.
     * All buckets are left-inclusive and right-exclusive, except the right most bucket which is right-inclusive.
     * If item is out of range of buckets returns -1
     */
    int indexOf(final double item);

    /**
     * @return a new array with a sorted list of the bucket boundaries
     */
    double[] getBoundaries();
}
