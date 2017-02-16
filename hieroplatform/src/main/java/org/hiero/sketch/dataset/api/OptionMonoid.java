package org.hiero.sketch.dataset.api;

/**
 * A simple monoid with two elements: null and some fixed object of type T.
 * null is the neutral element.
 * @param <T> Type of value.
 */
public class OptionMonoid<T> implements IMonoid<T> {
    @Override
    public T zero() { return null; }

    /**
     * Add two values.  If both values are not null, they are expected
     * to be the same value.  This is not checked, since it could be expensive.
     * @param left  Null or some value of type T.
     * @param right Null or some value of type T.
     * @return null if both are null, or else the non-null value.
     */
    @Override
    public T add(final T left, final T right) {
        return (left == null) ? right : left;
    }
}
