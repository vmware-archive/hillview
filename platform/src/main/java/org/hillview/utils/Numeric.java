package org.hillview.utils;

public class Numeric {

    /**
     * Returns the base-2 log of x, rounded down to the nearest long.
     */
    public static long longLog2(long x) {
        if (x <= 0) throw new RuntimeException("Attempted to take the log of a negative value: " + x);
        return (long)(Math.floor(Math.log(x) / Math.log(2)));
    }
}
