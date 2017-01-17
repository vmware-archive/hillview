package org.hiero.sketch.spreadsheet;

class WinsAndLosses {
    public final int wins;
    public final int losses;

    public WinsAndLosses(final int wins, final int losses) {
        this.wins = wins;
        this.losses = losses;
    }

    public String toString() {
        return String.valueOf(this.wins) + ", " + String.valueOf(this.losses);
    }
}
