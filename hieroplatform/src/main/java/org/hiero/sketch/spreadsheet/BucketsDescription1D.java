package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import java.util.Arrays;

/**
 * MetaData for one dimensional buckets held by a histogram
 */
public class BucketsDescription1D implements IBucketsDescription1D {
    private final double minValue;
    private final double maxValue;
    private final int numOfBuckets;
    private final double[] boundaries;

    /**
     * The assumption is that the all buckets are only left inclusive except the right one which
     * is right inclusive. Boundaries has to be strongly sorted.
     */
    public BucketsDescription1D(@NonNull final double[] boundaries) {
        if (boundaries.length == 0)
            throw new IllegalArgumentException("Boundaries of buckets can't be empty");
        if (!isSorted(boundaries))
            throw new IllegalArgumentException("Boundaries of buckets have to be sorted");
        this.boundaries = new double[boundaries.length];
        System.arraycopy(boundaries, 0, this.boundaries, 0, boundaries.length);
        this.numOfBuckets = boundaries.length - 1;
        this.minValue = this.boundaries[0];
        this.maxValue = this.boundaries[this.numOfBuckets];
    }

    /**
     * Checks that an array is strongly sorted
     */
    private static boolean isSorted(final double[] a) {
        for (int i = 0; i < (a.length - 1); i++)
            if (a[i] > a[i + 1])
                return false;
        return true;
    }

    @Override
    public int indexOf(final double item) {
        if ((item < this.minValue) || (item > this.maxValue))
            return -1;
        if (item == this.maxValue)
            return this.numOfBuckets - 1;
        int lo = 0;
        int hi = this.boundaries.length - 1;
        while (lo <= hi) {
            int mid = lo + ((hi - lo) / 2);
            if (item < this.boundaries[mid]) hi = mid ;
            else if (item >= this.boundaries[mid + 1]) lo = mid;
            else return mid;
        }
        throw new IllegalStateException("bug in the indexOf function");
    }

    @Override
    public double getLeftBoundary(int index) {
        if ((index < 0) || (index >= this.numOfBuckets))
            throw new IllegalArgumentException("Bucket index out of range");
        return this.boundaries[index];
    }

    @Override
    public double getRightBoundary(final int index) {
        if (index == (this.numOfBuckets - 1))
            return this.maxValue;
        return this.getLeftBoundary(index + 1);
    }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        BucketsDescription1D that = (BucketsDescription1D) o;

        if (Double.compare(that.minValue, this.minValue) != 0) return false;
        if (Double.compare(that.maxValue, this.maxValue) != 0) return false;
        if (this.numOfBuckets != that.numOfBuckets) return false;
        return Arrays.equals(this.boundaries, that.boundaries);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(this.minValue);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(this.maxValue);
        result = (31 * result) + (int) (temp ^ (temp >>> 32));
        result = (31 * result) + this.numOfBuckets;
        result = (31 * result) + Arrays.hashCode(this.boundaries);
        return result;
    }
}