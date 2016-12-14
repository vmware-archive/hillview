package org.hiero.sketch.spreadsheet;

/**
 * MetaData for one dimensional buckets of equal size
 */
public class BucketsDescriptionEqSize implements IBucketsDescription1D {

    private final double minValue;
    private final double maxValue;
    private final int numOfBuckets;


    public BucketsDescriptionEqSize(final double minValue, final double maxValue, final int numOfBuckets) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.numOfBuckets = numOfBuckets;
    }

    @Override
    public int indexOf(final double  item) {
        if (item < this.minValue || item > this.maxValue)
            return -1;
        if (item == this.maxValue)
            return this.numOfBuckets - 1;
        return (int) (this.numOfBuckets * (item - this.minValue)) / (int) (this.maxValue - this.minValue);
    }

    @Override
    public double getLeftBoundary(final int index) {
        if (index < 0 || index >= this.numOfBuckets)
            throw new IllegalArgumentException("Bucket index out of range");
        return this.minValue + (((this.maxValue - this.minValue) * index) / this.numOfBuckets);
    }

    @Override
    public double getRightBoundary(final int index) {
        if (index == (this.numOfBuckets - 1))
            return this.maxValue;
        return this.getLeftBoundary(index + 1);
    }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }

    @Override
    public double[] getBoundaries() {
        double[] result = new double[this.numOfBuckets + 1];
        double curr = this.minValue;
        for (int i = 0; i <= this.numOfBuckets; i++) {
            result[i] = curr;
            curr += (this.maxValue - this.minValue) / this.numOfBuckets;
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BucketsDescriptionEqSize that = (BucketsDescriptionEqSize) o;

        if (Double.compare(that.minValue, minValue) != 0) return false;
        if (Double.compare(that.maxValue, maxValue) != 0) return false;
        return numOfBuckets == that.numOfBuckets;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(minValue);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + numOfBuckets;
        return result;
    }
}
