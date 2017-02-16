package org.hiero.sketch.dataset.api;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A Partial result with a value from a monoid is also a monoid.  This class implements
 * the induced monoid over PartialResult[T]
 * @param <T> Type of value from a monoid.
 */
public class PartialResultMonoid<T> implements IMonoid<PartialResult<T>> {
    /**
     * Monoid over values of type T.
     */
    @NonNull
    private final IMonoid<T> monoid;

    public PartialResultMonoid(@NonNull final IMonoid<T> monoid) {
        this.monoid = monoid;
    }

    @NonNull public PartialResult<T> zero() {
        return new PartialResult<T>(0.0, this.monoid.zero());
    }

    @Override
    @NonNull public PartialResult<T> add(@NonNull final PartialResult<T> left,
                                @NonNull final PartialResult<T> right) {
        return new PartialResult<T>(left.deltaDone + right.deltaDone,
                this.monoid.add(left.deltaValue, right.deltaValue));
    }
}