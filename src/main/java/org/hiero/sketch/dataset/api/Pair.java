package org.hiero.sketch.dataset.api;

import java.io.Serializable;

public class Pair<T, S> implements Serializable {
    public final T first;
    public final S second;

    public Pair(final T first, final S second) {
        this.first = first;
        this.second = second;
    }
}
