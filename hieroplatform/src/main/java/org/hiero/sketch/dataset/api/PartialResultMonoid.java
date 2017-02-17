package org.hiero.sketch.dataset.api;

import javax.annotation.Nonnull;

/**
 * A Partial result with a value from a monoid is also a monoid.
 * @param <T> Type of value from a monoid.
 */
public class PartialResultMonoid<T> implements IMonoid<PartialResult<T>> {

    private final IMonoid<T> monoid;

    public PartialResultMonoid( final IMonoid<T> monoid) {
        this.monoid = monoid;
    }

    public PartialResult<T> zero() {
        return new PartialResult<T>(0.0, this.monoid.zero());
    }

    @Override
    public PartialResult<T> add( final PartialResult<T> left,
                                 final PartialResult<T> right) {
        return new PartialResult<T>(left.deltaDone + right.deltaDone,
                this.monoid.add(left.deltaValue, right.deltaValue));
    }
}