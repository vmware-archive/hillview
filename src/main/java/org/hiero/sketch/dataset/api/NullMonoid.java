package org.hiero.sketch.dataset.api;

/**
 * A simple monoid with two elements: null and some object of type T.
 * null is the neutral element.
 * @param <T> Type of value.
 */
public class NullMonoid<T> implements IMonoid<T> {
    @Override
    public T zero() { return null; }

    @Override
    public T add(final T left, final T right) {
        return (left == null) ? right : left;
    }
}
