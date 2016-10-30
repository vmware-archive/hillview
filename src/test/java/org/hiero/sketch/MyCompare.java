package org.hiero.sketch;

import java.util.Comparator;
import java.util.Objects;

/**
 * Created by parik on 10/20/16.
 */
public class MyCompare implements Comparator<Integer> {
    private MyCompare() {}

    static MyCompare instance = new MyCompare();

    @Override
    public int compare(Integer x, Integer y) {
        if (x > y)
            return 1;
        else if (Objects.equals(x, y))
            return 0;
        else
            return -1;
    }
}
