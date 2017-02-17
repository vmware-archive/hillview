package org.hiero.sketch.dataset.api;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * A simple polymorphic pair.
 * (How come Java does not have such a class built-in?)
 * Technically this class is serializable only if both T and S are, but there is no simple
 * way to check it statically, and we don't want a separate SerializablePair class.
 * @param <T>  First element in the pair.
 * @param <S>  Second element in the pair.
 */
public class Pair<T, S> implements Serializable {
    @Nullable
    public final T first;
    @Nullable
    public final S second;

    public Pair(@Nullable final T first, @Nullable final S second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(@Nullable final Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;
        final Pair<?, ?> pair = (Pair<?, ?>) o;
        if ((this.first != null) ? !this.first.equals(pair.first) : (pair.first != null)) return false;
        return (this.second != null) ? this.second.equals(pair.second) : (pair.second == null);
    }

    @Override
    public int hashCode() {
        int result = (this.first != null) ? this.first.hashCode() : 0;
        result = (31 * result) + ((this.second != null) ? this.second.hashCode() : 0);
        return result;
    }
}
