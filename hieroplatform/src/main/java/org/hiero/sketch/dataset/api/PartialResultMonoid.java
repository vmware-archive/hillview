package org.hiero.sketch.dataset.api;

import org.hiero.utils.Converters;

import javax.annotation.Nullable;

/**
 * A Partial result with a value from a monoid is also a monoid.  This class implements
 * the induced monoid over PartialResult[T]
 * @param <T> Type of value from a monoid.
 */
public class PartialResultMonoid<T> implements IMonoid<PartialResult<T>> {
    /**
     * Monoid over values of type T.
     */
    private final IMonoid<T> monoid;

    public PartialResultMonoid( final IMonoid<T> monoid) {
        this.monoid = monoid;
    }

    @Nullable
    public PartialResult<T> zero() {
        return new PartialResult<T>(0.0, this.monoid.zero());
    }

    @Override @Nullable
    public PartialResult<T> add(@Nullable PartialResult<T> left,
                                @Nullable PartialResult<T> right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        return new PartialResult<T>(left.deltaDone + right.deltaDone,
                this.monoid.add(left.deltaValue, right.deltaValue));
    }
}
