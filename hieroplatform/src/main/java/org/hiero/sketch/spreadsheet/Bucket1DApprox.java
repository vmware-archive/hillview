package org.hiero.sketch.spreadsheet;

/**   NOT USED
 * A Bucket class for one dimensional approximate buckets. Contains a normalization factor, deviation and error.
 */
public class Bucket1DApprox extends BaseBucket1D {
    private long mapSize;
    private int sampleSize;

    public Bucket1DApprox() {
        super();
    }

    public long getRealCount() { return super.getCount(); }

    public long getAppCount(){
        if (this.sampleSize == 0)
            return 0;
        return  (super.getCount() * this.mapSize) / this.sampleSize;
    }

    public Bucket1DApprox union(final BaseBucket1D otherBucket) {
        Bucket1DApprox uBucket = new Bucket1DApprox();
        super.union(otherBucket, uBucket);
        if (otherBucket instanceof Bucket1DApprox) {
            uBucket.mapSize = this.mapSize + ((Bucket1DApprox) otherBucket).mapSize;
            uBucket.sampleSize = this.sampleSize + ((Bucket1DApprox) otherBucket).sampleSize;
        }
        return null;
    }
}
