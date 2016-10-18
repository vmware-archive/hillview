package org.hiero.sketch.dataset.api;

import java.util.function.Function;

/**
 * Describes a sketch computation on a dataset of type T.
 * @param <T> Input data type.
 * @param <R> Output data type.
 */
public abstract class Sketch<T, R> {
    IMonoid<R> monoid;

    public abstract R create(T data, IProgressReporter reporter, CancellationToken ct);
}
