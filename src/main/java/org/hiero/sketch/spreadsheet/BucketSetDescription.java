package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Contains the MetaData for the buckets held by an histogram
 */
public class BucketSetDescription {
    public final int numOfBuckets;
    public final boolean equalSizeBukets;
    public final double minValue;
    public final double maxValue;
    public final double[] boundaries; //todo: Change the array to a unmodified list of some kind

    public BucketSetDescription(final double minValue, final double maxValue, final int numOfBuckets) {
        this.numOfBuckets = numOfBuckets;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.equalSizeBukets = true;
        this.boundaries = new double[numOfBuckets + 1];
        for (int i = 0; i < (numOfBuckets + 1); i++)
            this.boundaries[i] = minValue + (((maxValue - minValue) * i) / numOfBuckets);
    }

    /** The assumption is that the all buckets are only left inclusive except the right one which
     *  is right inclusive. Boundaries has to be strongly sorted.
     */
    public BucketSetDescription(@NonNull final double[] boundaries) {
        if (!isSorted(boundaries))
            throw new IllegalArgumentException("Boundaries of buckets have to be sorted");
        this.boundaries = boundaries; //todo The class should create its own array?
        this.minValue = boundaries[0];
        this.maxValue = boundaries[boundaries.length - 1];
        this.equalSizeBukets = false;
        this.numOfBuckets = boundaries.length - 1;
    }

    /**Checks that an array is strongly sorted
     */
    private static boolean isSorted(final double[] a) {
        for (int i = 0; i < (a.length - 1); i++) {
            if (a[i] > a[i + 1]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return index i such that a[i] <= key < a[i+1]. With the exception of the largest bucket
     * in which case the right boundary is inclusive. If key < a[0] or key > a[length] returns -1
     * Note that return is in [0..a.length-2].
     */
    public int indexOf(final double item) {
        if (this.equalSizeBukets) // if all buckets are equal a div operation suffices
            return (int) (this.numOfBuckets*(item-this.minValue))/(int) (this.maxValue - this.minValue);
        // todo: Currently linear scan. Change to Binary Search.
        if (item < this.boundaries[0])
            return -1;
        int i = 0;
        while (i < (this.boundaries.length - 1)) {
            if (item < this.boundaries[i + 1])
                return i;
            i++;
        }
        if (item == this.boundaries[this.boundaries.length - 1])
            return this.boundaries.length - 2;
        return -1;
    }

}
