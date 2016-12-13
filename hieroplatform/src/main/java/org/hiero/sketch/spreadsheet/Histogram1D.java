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

    private final Bucket[] buckets;

    private int missingData;

    private int outOfRange;

    final private IBucketsDescription1D bucketDescription;

    public Histogram1D(final @NonNull IBucketsDescription1D bucketDescription) {
        this.bucketDescription = bucketDescription;
        this.buckets = new Bucket[bucketDescription.getNumOfBuckets()];
        createBuckets();
    }

    private void createBuckets() {
        for (int i = 0; i < this.bucketDescription.getNumOfBuckets(); i++) {
            this.buckets[i] = new Bucket(this.bucketDescription.getLeftBoundary(i), true,
                    this.bucketDescription.getRightBoundary(i + 1),
                    (i == (this.bucketDescription.getNumOfBuckets() - 1)));
        }
    }

/*
    public Histogram1D(BucketsDescription1D bucketdescription, @NonNull final IColumn column,
                       @NonNull final IMembershipSet membershipSet,
                       @NonNull final IStringConverter converter) {
        this.bucketDescription = bucketdescription;
        this.buckets = new Bucket[bucketdescription.numOfBuckets]; //todo: create Buckets
        this.createHistogram(column, membershipSet, converter);
    }
*/
    /**
     * Creates the histogram explicitly and in full.
     */
    private void createHistogram(final IColumn column, final IMembershipSet membershipSet
                                ,final IStringConverter converter ) {
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (column.isMissing(currRow))
                this.missingData++;
            else {
                int index = this.bucketDescription.indexOf(currRow);
                if (index >= 0)
                    this.buckets[index].add(column.asDouble(currRow, converter),column.getObject(currRow));
                else this.outOfRange++;
            }
            currRow = myIter.getNextRow();
        }
    }
}
