package org.hiero.sketch.spreadsheet;

/**
 * A Bucket to be used as part of a histogram.
 */
public class Bucket {
    private final double leftBoundry;
    private final double  rightBoundry;
    private final boolean leftInclusive;
    private final boolean rightInclusive;

    public Bucket(final double leftBoundry, final boolean leftInclusive,
                  final double rightBoundry, final boolean rightInclusive) {
        if (leftBoundry > rightBoundry)
            throw new IllegalArgumentException("Left boundry cannot be greater than right boundry");
        else if ((leftBoundry == rightBoundry) && !(leftInclusive && rightInclusive))
            throw new IllegalArgumentException("Bucket defined over empty set");
        this.leftBoundry = leftBoundry;
        this.rightBoundry = rightBoundry;
        this.leftInclusive = leftInclusive;
        this.rightInclusive = rightInclusive;
    }

    public Bucket(final double leftBoundry, final double rightBoundry) {
         this(leftBoundry, true, rightBoundry, false);
    }

    public boolean inBucket(final double item) {

        if ((item < this.leftBoundry) || (item > this.rightBoundry))
            return false;
        if (item == this.leftBoundry)
            return this.leftInclusive;
        if (item == this.rightBoundry)
            return this.rightInclusive;
        return true;
    }
}
