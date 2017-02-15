package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

/**
 * A 2 dimension histogram where each bucket is a Bucket2D object. Designed to be used for
 * visualizations where we need to have more information on each bucket such as the minimum and
 * maximum values with their respective objects. The 1D histograms of missing items are composed
 * of (1D) bucket objects
 */
public class Histogram2DHeavy {
    private final Bucket2D[][] buckets;
    private long missingData;
    private long outOfRange;
    private final IBucketsDescription1D bucketDescDim1;
    private final IBucketsDescription1D bucketDescDim2;
    private boolean initialized;
    private Histogram1D histogramMissingD1; // hist of items that are missing in D2
    private Histogram1D histogramMissingD2; // hist of items that are missing in D1
    private long totalsize;


    public Histogram2DHeavy(final @NonNull IBucketsDescription1D buckets1, final @NonNull IBucketsDescription1D buckets2) {
        this.bucketDescDim1 = buckets1;
        this.bucketDescDim2 = buckets2;
        this.histogramMissingD1 = new Histogram1D(this.bucketDescDim1);
        this.histogramMissingD2 = new Histogram1D(this.bucketDescDim2);
        this.buckets = new Bucket2D[buckets1.getNumOfBuckets()][buckets2.getNumOfBuckets()];
        for (int i = 0; i < this.bucketDescDim1.getNumOfBuckets(); i++)
            for (int j = 0; j < this.bucketDescDim2.getNumOfBuckets(); j++)
                this.buckets[i][j] = new Bucket2D();
        this.initialized = false;
        this.totalsize = 0;
    }

    public void createSampleHistogram(final IColumn columnD1, final IColumn columnD2,
                                      final IStringConverter converterD1, final IStringConverter converterD2,
                                      final IMembershipSet membershipSet, double sampleRate) {
        this.createHistogram(columnD1, columnD2, converterD1, converterD2, membershipSet.sample(sampleRate));
    }

    public void createSampleHistogram(final IColumn columnD1, final IColumn columnD2,
                                      final IStringConverter converterD1, final IStringConverter converterD2,
                                      final IMembershipSet membershipSet, double sampleRate, long seed) {
        this.createHistogram(columnD1, columnD2, converterD1, converterD2, membershipSet.sample(sampleRate, seed));
    }

    public Histogram1D getMissingHistogramD1() { return this.histogramMissingD1; }

    public long getSize() { return this.totalsize; }

    public Histogram1D getMissingHistogramD2() { return this.histogramMissingD2; }

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
            boolean isMissingD1 = columnD1.isMissing(currRow);
            boolean isMissingD2 = columnD2.isMissing(currRow);
            if (isMissingD1 || isMissingD2) {
                if (!isMissingD1)  //only column 2 is missing
                    this.histogramMissingD1.addItem(columnD1.asDouble(currRow, converterD1),
                            columnD1.getObject(currRow));
                else if (!isMissingD2) // only column 1 is missing
                    this.histogramMissingD2.addItem(columnD2.asDouble(currRow, converterD2),
                            columnD2.getObject(currRow));
                else
                    this.missingData++; // both are missing
            }
            else {
                double val1 = columnD1.asDouble(currRow,converterD1);
                double val2 = columnD2.asDouble(currRow,converterD2);
                int index1 = this.bucketDescDim1.indexOf(val1);
                int index2 = this.bucketDescDim2.indexOf(val2);
                if ((index1 >= 0) && (index2 >= 0)) {
                    this.buckets[index1][index2].add(val1, columnD1.getObject(currRow),
                            val2, columnD2.getObject(currRow));
                    this.totalsize++;
                }
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
    public Bucket2D getBucket(final int index1, final int index2) { return this.buckets[index1][index2]; }

    /**
     * @param  otherHistogram with the same bucketDescription
     * @return a new Histogram which is the union of this and otherHistogram
     */
    public Histogram2DHeavy union( @NonNull Histogram2DHeavy otherHistogram) {
        if ((!this.bucketDescDim1.equals(otherHistogram.bucketDescDim1))
                || (!this.bucketDescDim2.equals(otherHistogram.bucketDescDim2)))
            throw new IllegalArgumentException("Histogram union without matching buckets");
        Histogram2DHeavy unionH = new Histogram2DHeavy(this.bucketDescDim1, this.bucketDescDim2);
        for (int i = 0; i < unionH.bucketDescDim1.getNumOfBuckets(); i++)
            for (int j = 0; j < unionH.bucketDescDim2.getNumOfBuckets(); j++)
                unionH.buckets[i][j] = this.buckets[i][j].union(otherHistogram.buckets[i][j]);
        unionH.missingData= this.missingData + otherHistogram.missingData;
        unionH.outOfRange = this.outOfRange + otherHistogram.outOfRange;
        unionH.initialized = true;
        unionH.totalsize = this.totalsize + otherHistogram.totalsize;
        unionH.histogramMissingD1 = this.histogramMissingD1.union(otherHistogram.histogramMissingD1);
        unionH.histogramMissingD2 = this.histogramMissingD2.union(otherHistogram.histogramMissingD2);
        return unionH;
    }
}
