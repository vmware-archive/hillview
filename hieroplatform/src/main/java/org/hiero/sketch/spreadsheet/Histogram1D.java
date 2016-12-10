package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

/**
 * One Dimensional histogram. Does not contain the column and membershipMap
 * todo: work in progress
 */
public class Histogram1D implements IHistogram {
   // final private IColumn column;
   // private IMembershipSet membershipSet;
   // final private double[] boundaries;
    private final Bucket[] buckets;
   // final private IStringConverter converter;
    private int missingData;
    private int outOfRange;
    final private BucketSetDescription bucketDescription;

    public Histogram1D(@NonNull final BucketSetDescription bucketDescription) {
        this.bucketDescription = bucketDescription;
        this.buckets = new Bucket[bucketDescription.numOfBuckets];
        createBuckets();
    }

    private void createBuckets() {
        for (int i = 0; i < this.bucketDescription.numOfBuckets; i++) {
            this.buckets[i] = new Bucket(this.bucketDescription.boundaries[i], true,
                    this.bucketDescription.boundaries[i + 1],
                    (i == (this.bucketDescription.numOfBuckets - 1)));
        }
    }


    public Histogram1D(BucketSetDescription bucketdescription, @NonNull final IColumn column,
                       @NonNull final IMembershipSet membershipSet,
                       @NonNull final IStringConverter converter) {
        this.bucketDescription = bucketdescription;
        this.buckets = new Bucket[bucketdescription.numOfBuckets]; //todo: create Buckets
        this.createHistogram(column, membershipSet, converter);
    }

    /**
     * Creates the histogram explicitly and in full.
     */
    private void createHistogram(final IColumn column, final IMembershipSet membershipSet
                                ,final IStringConverter converter ) {
        final IRowIterator myIter = membershipSet.getIterator();
        int currIndex = myIter.getNextRow();
        while (currIndex >= 0) {
            if (column.isMissing(currIndex))
                this.missingData++;
            else
                placeInBucket(column.asDouble(currIndex, converter));
            currIndex = myIter.getNextRow();
        }
    }

    private void placeInBucket(final double entry) {
        final int index = this.bucketDescription.indexOf(entry);
        if (index >= 0)
            this.buckets[index].add(entry);
        else this.outOfRange++;
    }
}
