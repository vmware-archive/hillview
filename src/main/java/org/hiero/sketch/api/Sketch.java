package org.hiero.sketch.api;

/**
 * Describes a sketch computation on a dataset of type T.
 * @param <T> Input data type.
 * @param <R> Output data type.
 */
public abstract class ISketch<T, R> {
    IMonoid<R> monoid;

    public abstract R create(IMap<T, R> creator);
}
