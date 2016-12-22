package org.hiero.sketch.spreadsheet;

public class ApproxRank {
    private final int lower;
    private final int upper;

    public ApproxRank(final int lower, final int upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public int getLower() {
        return this.lower;
    }

    public int getUpper() {
        return this.upper;
    }

    public String toString() {
        return String.valueOf(this.lower) + ", " + String.valueOf(this.upper);
    }
}
