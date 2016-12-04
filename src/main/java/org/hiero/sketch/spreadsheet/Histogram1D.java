package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

public class Histogram1D implements IHistogram {
    final private IColumn column;
    final private IMembershipSet membershipSet;
    final private double[] boundaries;
    final private int[] buckets;
    final private IStringConverter converter;
    private int missingData;
    private int outOfRange;

    public Histogram1D(@NonNull final IColumn column, @NonNull final IMembershipSet membershipSet,
                       @NonNull final IStringConverter converter, @NonNull final double[] boundaries) {
        if (!isSorted(boundaries))
            throw new IllegalArgumentException("Bucket array has to be sorted");
        this.column = column;
        this.membershipSet = membershipSet;
        this.boundaries = boundaries;
        this.buckets = new int[boundaries.length - 1];
        this.converter = converter;
        this.createHistogram();
    }

    /**
     * Creates the histogram explicitly and in full.
     */
    private void createHistogram() {
        final IRowIterator myIter = this.membershipSet.getIterator();
        int currIndex = myIter.getNextRow();
        while (currIndex >= 0) {
            if (this.column.isMissing(currIndex))
                this.missingData++;
            else
                placeInBucket(this.column.asDouble(currIndex, this.converter));
            currIndex = myIter.getNextRow();
        }
    }

    private void placeInBucket(final double entry) {
        final int index = Histogram1D.indexOf(this.boundaries, entry);
        if (index >= 0)
            this.buckets[index]++;
        else this.outOfRange++;
    }

    // Todo: Move to a base class at some point
    private static boolean isSorted(final double[] a) {
        for (int i = 0; i < (a.length - 1); i++) {
            if (a[i] > a[i + 1]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param a array of sorted doubles, represent the buckets
     * @return index i such that a[i] <= key < a[i+1]. With the exception of the largest bucket
     * in which case the right boundary is inclusive. If key < a[0] or key > a[length] returns -1
     * Note that return is in [0..a.length-2].
     * todo: Move to a base class at some point. Perhaps implement as binary search.
     */
    private static int indexOf(final double[] a, final double key) {
        if (key < a[0])
            return -1;
        int i = 0;
        while (i < (a.length - 1)) {
            if (key < a[i + 1])
                return i;
            i++;
        }
        if (key == a[a.length - 1])
            return a.length - 2;
        return -1;
    }

    @Override
    public int[] getBuckets() {
        return this.buckets;
    }
}
