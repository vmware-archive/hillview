package org.hiero.sketch;

import java.util.Comparator;
import java.util.Objects;

public class MyCompare implements Comparator<Integer> {
    private MyCompare() {}

    static final MyCompare instance = new MyCompare();

    @Override
    public int compare(final Integer x, final Integer y) {
        if (x > y)
            return 1;
        else if (Objects.equals(x, y))
            return 0;
        else
            return -1;
    }
}
