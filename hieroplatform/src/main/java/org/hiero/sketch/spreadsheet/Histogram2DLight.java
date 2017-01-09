package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

/**
 * An implementation of a 2 dimension histogram. It is designed assuming the number of buckets is very large, so there
 * are no bucket objects, nor min and max items per bucket, the only thing stored is counts.
 * The buckets are assumed to be equisize.
 */
public class Histogram2DLight {
    private final long[][] buckets;
    private long missingData;
    private long outOfRange;
    private final IBucketsDescription1D bucketDescDim1;
    private final IBucketsDescription1D bucketDescDim2;
    private boolean initialized;

    public Histogram2DLight(final @NonNull IBucketsDescription1D buckets1, final @NonNull IBucketsDescription1D buckets2) {
        this.bucketDescDim1 = buckets1;
        this.bucketDescDim2 = buckets2;
        this.buckets = new long[buckets1.getNumOfBuckets()][buckets2.getNumOfBuckets()]; // Automatically initialized to 0
        this.initialized = false;
    }

    /**
     * Creates the histogram explicitly and in full. Should be called at most once.
     */
    public void createHistogram(final IColumn columnD1, final IColumn columnD2,
                                final IStringConverter converterD1, final IStringConverter converterD2,
                                final IMembershipSet membershipSet) {
        if (this.initialized) //a histogram had already been created
            throw new IllegalAccessError("A histogram cannot be created twice");
        this.initialized = true;
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if ((columnD1.isMissing(currRow)) || (columnD2.isMissing(currRow)))
                this.missingData++;
            else {
                double val1 = columnD1.asDouble(currRow,converterD1);
                double val2 = columnD2.asDouble(currRow,converterD2);
                int index1 = this.bucketDescDim1.indexOf(val1);
                int index2 = this.bucketDescDim2.indexOf(val2);
                if ((index1 >= 0) && (index2 >= 0))
                    this.buckets[index1][index2]++;
                else this.outOfRange++;
            }
            currRow = myIter.getNextRow();
        }
    }

    public int getNumOfBucketsD1() { return this.bucketDescDim1.getNumOfBuckets(); }

    public int getNumOfBucketsD2() { return this.bucketDescDim2.getNumOfBuckets(); }

    public long getMissingData() { return this.missingData; }

    public long getOutOfRange() { return this.outOfRange; }

    /**
     * @return the index's bucket or null if not been initialized yet
     */
    public long getBucket(final int index1, final int index2) {
        if (!this.initialized)
            throw new IllegalArgumentException("bucket not initialized yet");
        if ((index1 < 0) || (index1 >= this.bucketDescDim1.getNumOfBuckets())
              || (index2 < 0) || (index2 >= this.bucketDescDim2.getNumOfBuckets()))
            throw new IllegalArgumentException("bucket index out of range");
        return this.buckets[index1][index2];
    }

    /**
     * @param  otherHistogram with the same bucketDescription
     * @return a new Histogram which is the union of this and otherHistogram
     */
    public Histogram2DLight union( @NonNull Histogram2DLight otherHistogram) {
        if ((!this.bucketDescDim1.equals(otherHistogram.bucketDescDim1))
            || (!this.bucketDescDim2.equals(otherHistogram.bucketDescDim2)))
            throw new IllegalArgumentException("Histogram union without matching buckets");
        if ((!this.initialized) || (!otherHistogram.initialized))
            throw new IllegalArgumentException("Uninitialized histogram cannot be part of a union");
        Histogram2DLight unionH = new Histogram2DLight(this.bucketDescDim1, this.bucketDescDim2);
        for (int i = 0; i < unionH.bucketDescDim1.getNumOfBuckets(); i++)
            for (int j = 0; j < unionH.bucketDescDim2.getNumOfBuckets(); j++)
                unionH.buckets[i][j] = this.buckets[i][j] + otherHistogram.buckets[i][j];
        unionH.missingData = this.missingData + otherHistogram.missingData;
        unionH.outOfRange = this.outOfRange + otherHistogram.outOfRange;
        return unionH;
    }
}
