package org.hiero.sketch.dataset.api;

import java.io.Serializable;

public class Pair<T, S> implements Serializable {
    public final T first;
    public final S second;

    public Pair(final T first, final S second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if (this.first != null ? !this.first.equals(pair.first) : pair.first != null) return false;
        return this.second != null ? this.second.equals(pair.second) : pair.second == null;

    }

    @Override
    public int hashCode() {
        int result = this.first != null ? this.first.hashCode() : 0;
        result = 31 * result + (this.second != null ? this.second.hashCode() : 0);
        return result;
    }
}
