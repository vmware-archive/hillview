package org.hiero.sketch.dataset.api;

/**
 * A monoid structure
 * @param <R> Concrete representation of a monoid's elements.
 */
public interface IMonoid<R> {
    R Zero();
    R Add(R left, R right);
}
