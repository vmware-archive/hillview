package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Base class for one dimensional buckets
 */
public abstract class BaseBucket1D {
    private Object minObject;
    private Object maxObject;
    private double minValue;
    private double maxValue;
    private long count;

    public BaseBucket1D() {
        this.minObject = null;
        this.maxObject = null;
        this.minValue = 0;
        this.maxValue = 0;
        this.count = 0;
    }

    private BaseBucket1D(final long count, final double minValue,
                     final double maxValue, final Object minObject, final Object maxObject) {
        this.count = count;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.minObject = minObject;
        this.maxObject = maxObject;
    }

    public Object getMinObject() { return this.minObject; }

    public Object getMaxObject() { return this.maxObject; }

    public double getMinValue() { return this.minValue; }

    public double getMaxValue() { return this.maxValue; }

    public long getCount() { return this.count; }

    public void add(final double item, @NonNull final Object currObject) {
        if (this.count == 0) {
            this.minValue = item;
            this.minObject = currObject;
            this.maxValue = item;
            this.maxObject = currObject;
        } else if (item < this.minValue) {
            this.minValue = item;
            this.minObject = currObject;
        } else if (item > this.maxValue) {
            this.maxValue = item;
            this.maxObject = currObject;
        }
        this.count++;
    }

    public boolean isEmpty() { return this.count == 0; }

    /**
     *
     * @param otherBucket to perform a union with this
     * @param unionBucket the bucket which is the union, instantiated by the subclass
     */
    public void union(@NonNull final BaseBucket1D otherBucket, @NonNull BaseBucket1D unionBucket) {
        if (this.minValue < otherBucket.minValue) {
            unionBucket.minValue = this.minValue;
            unionBucket.minObject = this.minObject;
        } else {
            unionBucket.minValue = otherBucket.minValue;
            unionBucket.minObject = otherBucket.minObject;
        }
        if (this.maxValue > otherBucket.maxValue) {
            unionBucket.maxValue = this.maxValue;
            unionBucket.maxObject = this.maxObject;
        } else {
            unionBucket.maxValue = otherBucket.maxValue;
            unionBucket.maxObject = otherBucket.maxObject;
        }
        unionBucket.count = this.count + otherBucket.count;
    }
}
