package org.hiero.sketch.spreadsheet;

/**
 * Abstract class for bucket set metadata
 */
public abstract class BaseBucketsDescription1D implements IBucketsDescription1D{

    public final int numOfBuckets;

    public BaseBucketsDescription1D(int numOfBuckes) { this.numOfBuckets = numOfBuckes; }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }
}
