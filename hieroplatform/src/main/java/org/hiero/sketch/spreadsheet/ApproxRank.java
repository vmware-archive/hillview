package org.hiero.sketch.spreadsheet;

class ApproxRank {
    public final int lower;
    public final int upper;

    public ApproxRank(final int lower, final int upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public String toString() {
        return String.valueOf(this.lower) + ", " + String.valueOf(this.upper);
    }
}
