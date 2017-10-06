package org.hillview.utils;

import java.util.Comparator;

/**
 * Wraps an instance of an integer.
 */
public class MutableInteger {
    public static final MutableIntegerComparator COMPARATOR = new MutableIntegerComparator();
    private int val;

    public MutableInteger(int val) {
        this.val = val;
    }

    public int get() {
        return val;
    }

    public void set(int val) {
        this.val = val;
    }

    public static class MutableIntegerComparator implements Comparator<MutableInteger> {
        @Override
        public int compare(final MutableInteger o1, final MutableInteger o2) {
            return Integer.compare(o1.get(), o2.get());
        }
    }
}
