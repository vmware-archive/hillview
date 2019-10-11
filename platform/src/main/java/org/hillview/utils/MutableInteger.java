package org.hillview.utils;

import java.util.Comparator;

/**
 * A wrapper around an integer value, which is used for comparing two instances
 */
public class MutableInteger {
    public static final MutableIntegerComparator COMPARATOR = new MutableIntegerComparator();
    private int val;

    public MutableInteger(int val) {
        this.set(val);
    }

    public int get() {
        return val;
    }
    public void set(int val) {
        this.val = val;
    }

    public static class MutableIntegerComparator implements Comparator<MutableInteger> {
        // Comparison is done only using the integer value.
        @Override
        public int compare(final MutableInteger o1, final MutableInteger o2) {
            return Integer.compare(o1.get(), o2.get());
        }
    }
}
