package org.hiero.sketch.dataset.api;

import rx.Observable;

import java.io.Serializable;

/**
 * A monoid structure.
 * @param <R> Type of data representing an element of the monoid.
 */
public interface IMonoid<R> extends Serializable {
    R zero();
    R add(R left, R right);

    static <T> Observable<T> scan(final Observable<T> obs, final IMonoid<T> monoid) {
        return obs.scan(monoid.zero(), monoid::add);
    }
}
