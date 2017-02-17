package org.hiero.sketch.dataset.api;

import java.io.Serializable;

/**
 * A simple polymorphic pair with non-nullable components.
 * (How come Java does not have such a class built-in?)
 * Technically this class is serializable only if both T and S are, but there is no simple
 * way to check it statically, and we don't want a separate SerializablePair class.
 * @param <T>  First element in the pair.
 * @param <S>  Second element in the pair.
 */
public class Pair<T, S> implements Serializable {
    public final T first;
    public final S second;

    public Pair(final T first, final S second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;
        final Pair<?, ?> pair = (Pair<?, ?>) o;
        if (!this.first.equals(pair.first)) return false;
        return this.second.equals(pair.second);
    }

    @Override
    public int hashCode() {
        int result = this.first.hashCode();
        result = (31 * result) + this.second.hashCode();
        return result;
    }
}
