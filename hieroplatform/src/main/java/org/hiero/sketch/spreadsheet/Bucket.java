package org.hiero.sketch.spreadsheet;

/**
 * A Bucket to be used as part of a histogram.
 */
public class Bucket {
    private final double leftBoundary;
    private final double  rightBoundary;
    private final boolean leftInclusive;
    private final boolean rightInclusive;
    private Object minObject;
    private Object maxObject;
    private double minValue;
    private double maxValue;
    private long count;

    public Bucket(final double leftBoundary, final boolean leftInclusive,
                  final double rightBoundary, final boolean rightInclusive) {
        if (leftBoundary > rightBoundary)
            throw new IllegalArgumentException("Left boundry cannot be greater than right boundry");
        else if ((leftBoundary == rightBoundary) && !(leftInclusive && rightInclusive))
            throw new IllegalArgumentException("Bucket defined over empty set");
        this.leftBoundary = leftBoundary;
        this.rightBoundary = rightBoundary;
        this.leftInclusive = leftInclusive;
        this.rightInclusive = rightInclusive;
    }

    public Object getMinObject() { return this.minObject; }

    public Object getMaxObject() { return this.maxObject; }

    public double getMinValue() { return this.minValue; }

    public double getMaxValue() { return this.maxValue; }

    public long getCount() { return this.count; }

    public Bucket(final double leftBoundary, final double rightBoundary) {
         this(leftBoundary, true, rightBoundary, false);
    }

    public boolean inBucket(final double item) {

        if ((item < this.leftBoundary) || (item > this.rightBoundary))
            return false;
        if (item == this.leftBoundary)
            return this.leftInclusive;
        if (item == this.rightBoundary)
            return this.rightInclusive;
        return true;
    }

    public void add(final double item, Object currObject) {
        this.count++;
        if (item < this.minValue)
        {
            this.minValue = item;
            this.minObject = currObject;
        }
        if (item > this.maxValue)
        {
            this.maxValue = item;
            this.maxObject = currObject;
        }
    }
}
