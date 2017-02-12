package org.hiero.sketch.spreadsheet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

/**
 * One Dimensional histogram. Does not contain the column and membershipMap
 */
public class Histogram1D extends BaseHist1D {
    private final Bucket1D[] buckets;
    private long missingData;
    private long outOfRange;
    private boolean initialized;

    public Histogram1D(final @NonNull IBucketsDescription1D bucketDescription) {
        super(bucketDescription);
        this.buckets = new Bucket1D[bucketDescription.getNumOfBuckets()];
        for (int i = 0; i < this.bucketDescription.getNumOfBuckets(); i++)
            this.buckets[i] = new Bucket1D();
        this.initialized = false;
    }

    /**
     * Creates the histogram explicitly and in full. Should be called at most once.
     */
    @Override
    public void createHistogram(final IColumn column, final IMembershipSet membershipSet,
                                final IStringConverter converter ) {
        if (this.initialized) //a histogram had already been created
            throw new IllegalAccessError("A histogram cannot be created twice");
        this.initialized = true;
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

    public void addItem(final double value, final Object item) {
        int index = this.bucketDescription.indexOf(value);
        if (index >= 0)
            this.buckets[index].add(value,item);
        else this.outOfRange++;
    }

    public long getMissingData() { return this.missingData; }

    public long getOutOfRange() { return this.outOfRange; }

    /**
     * @return the index's bucket or exception if doesn't exist.
     */
    public @NonNull Bucket1D getBucket(final int index) { return this.buckets[index]; }

    /**
     * @param  otherHistogram with the same bucketDescription
     * @return a new Histogram which is the union of this and otherHistogram
     */
    public @NonNull Histogram1D union( @NonNull Histogram1D otherHistogram) {
        if (!this.bucketDescription.equals(otherHistogram.bucketDescription))
            throw new IllegalArgumentException("Histogram union without matching buckets");
        Histogram1D unionH = new Histogram1D(this.bucketDescription);
        unionH.initialized = true;
        for (int i = 0; i < unionH.bucketDescription.getNumOfBuckets(); i++)
            unionH.buckets[i] = this.buckets[i].union(otherHistogram.buckets[i]);
        unionH.missingData = this.missingData + otherHistogram.missingData;
        unionH.outOfRange = this.outOfRange + otherHistogram.outOfRange;
        return unionH;
    }
}