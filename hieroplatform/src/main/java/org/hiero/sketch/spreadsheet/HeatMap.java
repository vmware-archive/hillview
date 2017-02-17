package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

import javax.annotation.Nullable;

/**
 * An implementation of a 2 dimension histogram. It is designed assuming the number of buckets is very large, so there
 * are no bucket objects, nor min and max items per bucket, the only thing stored is counts.
 * The buckets are assumed to have equal sizes.
 */
public class HeatMap {
    private final long[][] buckets;
    private long missingData; //number of items missing on both columns
    private long outOfRange;
    private final IBucketsDescription1D bucketDescDim1;
    private final IBucketsDescription1D bucketDescDim2;
    private Histogram1DLight histogramMissingD1; // hist of items that are missing in D2
    private Histogram1DLight histogramMissingD2; // hist of items that are missing in D1
    private boolean initialized;
    private long totalsize;

    public HeatMap(final IBucketsDescription1D buckets1,
                   final IBucketsDescription1D buckets2) {
        this.bucketDescDim1 = buckets1;
        this.bucketDescDim2 = buckets2;
        this.buckets = new long[buckets1.getNumOfBuckets()][buckets2.getNumOfBuckets()]; // Automatically initialized to 0
        this.initialized = false;
        this.histogramMissingD1 = new Histogram1DLight(this.bucketDescDim1);
        this.histogramMissingD2 = new Histogram1DLight(this.bucketDescDim2);
    }

    /**
     * Creates the histogram explicitly and in full. Should be called at most once.
     */
    public void createHistogram(final IColumn columnD1, final IColumn columnD2,
                                @Nullable final IStringConverter converterD1,
                                @Nullable final IStringConverter converterD2,
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
                    this.histogramMissingD1.addValue(columnD1.asDouble(currRow, converterD1));
                else if (!isMissingD2) // only column 1 is missing
                    this.histogramMissingD2.addValue(columnD2.asDouble(currRow, converterD2));
                else
                    this.missingData++; // both are missing
                }
            else {
                double val1 = columnD1.asDouble(currRow,converterD1);
                double val2 = columnD2.asDouble(currRow,converterD2);
                int index1 = this.bucketDescDim1.indexOf(val1);
                int index2 = this.bucketDescDim2.indexOf(val2);
                if ((index1 >= 0) && (index2 >= 0)) {
                    this.buckets[index1][index2]++;
                    this.totalsize++;
                }
                else this.outOfRange++;
            }
            currRow = myIter.getNextRow();
        }
    }

    public Histogram1DLight getMissingHistogramD1() { return this.histogramMissingD1; }

    public long getSize() { return this.totalsize; }

    public Histogram1DLight getMissingHistogramD2() { return this.histogramMissingD2; }

    public void createSampleHistogram(final IColumn columnD1, final IColumn columnD2,
                                      @Nullable final IStringConverter converterD1,
                                      @Nullable final IStringConverter converterD2,
                                      final IMembershipSet membershipSet, double sampleRate) {
        this.createHistogram(columnD1, columnD2, converterD1, converterD2, membershipSet.sample(sampleRate));
    }

    public void createSampleHistogram(final IColumn columnD1, final IColumn columnD2,
                                      @Nullable final IStringConverter converterD1,
                                      @Nullable final IStringConverter converterD2,
                                      final IMembershipSet membershipSet,
                                      double sampleRate, long seed) {
        this.createHistogram(columnD1, columnD2, converterD1, converterD2, membershipSet.sample(sampleRate, seed));
    }

    public int getNumOfBucketsD1() { return this.bucketDescDim1.getNumOfBuckets(); }

    public int getNumOfBucketsD2() { return this.bucketDescDim2.getNumOfBuckets(); }

    public long getMissingData() { return this.missingData; }

    public long getOutOfRange() { return this.outOfRange; }

    /**
     * @return the index's count
     */
    public long getCount(final int index1, final int index2) { return this.buckets[index1][index2]; }

    /**
     * @param  otherHeatmap with the same bucketDescriptions
     * @return a new Histogram which is the union of this and otherHeatmap
     */
    public HeatMap union( HeatMap otherHeatmap) {
        if ((!this.bucketDescDim1.equals(otherHeatmap.bucketDescDim1))
            || (!this.bucketDescDim2.equals(otherHeatmap.bucketDescDim2)))
            throw new IllegalArgumentException("Histogram union without matching buckets");
        HeatMap unionH = new HeatMap(this.bucketDescDim1, this.bucketDescDim2);
        for (int i = 0; i < unionH.bucketDescDim1.getNumOfBuckets(); i++)
            for (int j = 0; j < unionH.bucketDescDim2.getNumOfBuckets(); j++)
                unionH.buckets[i][j] = this.buckets[i][j] + otherHeatmap.buckets[i][j];
        unionH.missingData = this.missingData + otherHeatmap.missingData;
        unionH.outOfRange = this.outOfRange + otherHeatmap.outOfRange;
        unionH.totalsize = this.totalsize + otherHeatmap.totalsize;
        unionH.initialized = true;
        unionH.histogramMissingD1 = this.histogramMissingD1.union(otherHeatmap.histogramMissingD1);
        unionH.histogramMissingD2 = this.histogramMissingD2.union(otherHeatmap.histogramMissingD2);
        return unionH;
    }
}
