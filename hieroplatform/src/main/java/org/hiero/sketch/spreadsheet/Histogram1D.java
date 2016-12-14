package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

/**
 * One Dimensional histogram. Does not contain the column and membershipMap
 * todo: Add createApproxHistogram that takes error parameters and uses sampling
 */
public class Histogram1D {

    private final Bucket1D[] buckets;

    private long missingData;

    private long outOfRange;

    private final IBucketsDescription1D bucketDescription;

    public Histogram1D(final @NonNull IBucketsDescription1D bucketDescription) {
        this.bucketDescription = bucketDescription;
        this.buckets = new Bucket1D[bucketDescription.getNumOfBuckets()];
    }

    /**
     * Creates the histogram explicitly and in full. Should be called at most once.
     */
    public void createHistogram(final IColumn column, final IMembershipSet membershipSet
                                ,final IStringConverter converter ) {
        if (this.buckets[0] != null) //a histogram had already been created
            throw new IllegalAccessError("A histogram cannot be created twice");
        for (int i = 0; i < this.bucketDescription.getNumOfBuckets(); i++)
            this.buckets[i] = new Bucket1D();
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (column.isMissing(currRow))
                this.missingData++;
            else {
                double val = column.asDouble(currRow,converter);
                int index = this.bucketDescription.indexOf(val);
                if (index >= 0)
                    this.buckets[index].add(val,column.getObject(currRow));
                else this.outOfRange++;
            }
            currRow = myIter.getNextRow();
        }
    }

    public long getMissingData() { return this.missingData; }

    public long getOutOfRange() { return this.outOfRange; }

    /**
     * @return the index's bucket or null if not been initialized yet
     */
    public Bucket1D getBucket(final int index) {
        if ((index < 0) || (index >= this.bucketDescription.getNumOfBuckets()))
            throw new IllegalArgumentException("bucket index out of range");
        return this.buckets[index];
    }

    /**
     * @param  otherHistogram with the same bucketDescription
     * @return a new Histogram which is the union of this and otherHistogram
     */
    public Histogram1D union( @NonNull Histogram1D otherHistogram) {
        if (!this.bucketDescription.equals(otherHistogram.bucketDescription))
            throw new IllegalArgumentException("Histogram union without matching buckets");
        Histogram1D unionH = new Histogram1D(this.bucketDescription);
        for (int i = 0; i < unionH.bucketDescription.getNumOfBuckets(); i++)
            unionH.buckets[i] = this.buckets[i].union(otherHistogram.buckets[i]);
        unionH.missingData = this.missingData + otherHistogram.missingData;
        unionH.outOfRange = this.outOfRange + otherHistogram.outOfRange;
        return unionH;
    }
}