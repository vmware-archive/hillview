package org.hiero.sketch.spreadsheet;
import java.util.*;
/**
 * A Bucket to be used as part of a histogram.
 */
public class Bucket<T> {
    private final T leftBoundry;
    private final T rightBoundry;
    private final boolean leftInclusive;
    private final boolean rightInclusive;
    private final Comparator<T> greater;

    public Bucket(T leftBoundry, boolean leftInclusive,
                       T rightBoundry, boolean rightInclusive, Comparator<T> greater) {
        if (greater.compare(leftBoundry, rightBoundry) > 0)
            throw new IllegalArgumentException("Left boundry cannot be greater than right boundry");
        else if ((greater.compare(leftBoundry, rightBoundry) == 0 )
                && !(leftInclusive && rightInclusive))
            throw new IllegalArgumentException("Bucket defined over empty set");
        this.leftBoundry = leftBoundry;
        this.rightBoundry = rightBoundry;
        this.leftInclusive = leftInclusive;
        this.rightInclusive = rightInclusive;
        this.greater = greater;
    }

    public Bucket(T leftBoundry, T rightBoundry, Comparator<T> greater) {
         this(leftBoundry, true, rightBoundry, false, greater);
    }

    public boolean inBucket(T item) {
        final int leftValue = greater.compare(item, leftBoundry);
        final int rightValue = greater.compare(item, rightBoundry);

        if ((leftValue < 0) || (rightValue > 0))
            return false;
        if (leftValue == 0)
            return this.leftInclusive;
        if (rightValue == 0)
            return this.rightInclusive;
        return true;
    }
}
