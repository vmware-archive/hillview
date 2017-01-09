package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

/**
 * A one dimensional histogram that uses sampling to provide an approximation of the count.
 * todo: Add calculation of error bars
 */
public class Histogram1DApprox  {
    private final Bucket1D[] buckets;
    private long missingData;
    private long outOfRange;
    private final IBucketsDescription1D bucketDescription;
    private final double[] variance;  //unbiased estimates for fraction of items in the bucket!
    private final double[] expectation;
    private long sampleSize;
    private long universeSize;

    public Histogram1DApprox(final @NonNull IBucketsDescription1D bucketDescription) {
        this.bucketDescription = bucketDescription;
        this.buckets = new Bucket1D[bucketDescription.getNumOfBuckets()];
        this.variance = new double[bucketDescription.getNumOfBuckets()];
        this.expectation = new double[bucketDescription.getNumOfBuckets()];
    }

    public void createHistogram(final IColumn column, final IMembershipSet membershipSet,
                                final IStringConverter converter, final int sampleSize) {
        if (sampleSize < 0)
            throw new IllegalArgumentException("Sample size can't be negative");
        IMembershipSet sampleMap = membershipSet.sample(sampleSize);
        createHistogramHelper(column, sampleMap, converter);
        this.sampleSize = sampleSize;
        this.universeSize = membershipSet.getSize();
        for (int i = 0; i < this.bucketDescription.getNumOfBuckets(); i++ ) {
            final long currCount = this.buckets[i].getCount();
            this.expectation[i] = currCount / this.sampleSize;
            this.variance[i] = ((currCount * Math.pow((1 - this.expectation[i]),2))
                                + ((this.sampleSize - currCount) * Math.pow(this.expectation[i], 2)))
                                        * (1 / (this.sampleSize - 1));
        }

    }

    public long getSampleSize() { return this.sampleSize; }

    public long getUniverseSize() {return this.universeSize; }


    public long appCount(int i) {
        return (long) (this.expectation[i] * this.universeSize);
    }

    public double getVariance(int i) { return this.variance[i]; }

    public Histogram1DApprox union(Histogram1DApprox otherHistogram) {
        if (!this.bucketDescription.equals(otherHistogram.bucketDescription))
            throw new IllegalArgumentException("Histogram union without matching buckets");
        if ((this.buckets[0] == null) || (otherHistogram.buckets[0] == null) )
            throw new IllegalArgumentException("Uninitialized histogram cannot be part of a union");
        Histogram1DApprox unionH = new Histogram1DApprox(this.bucketDescription);
        unionH.sampleSize = this.sampleSize + otherHistogram.sampleSize;
        unionH.universeSize = this.universeSize + otherHistogram.universeSize;
        for (int i = 0; i < unionH.bucketDescription.getNumOfBuckets(); i++) {
            unionH.buckets[i] = this.buckets[i].union(otherHistogram.buckets[i]);
            unionH.expectation[i] = (this.appCount(i) + otherHistogram.appCount(i)) / unionH.sampleSize;
            //todo add the calculation of variance. Is it just adding it up?
        }
        unionH.missingData = this.missingData + otherHistogram.missingData;
        unionH.outOfRange = this.outOfRange + otherHistogram.outOfRange;
        return unionH;
    }

    private void createHistogramHelper(final IColumn column, final IMembershipSet membershipSet,
        final IStringConverter converter ) {
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
}
