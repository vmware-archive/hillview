package org.hiero.sketch.dataset.api;

/**
 * A closure that runs a computation on an object of type T
 * and returns an object of type S.
 * @param <T> Input type.
 * @param <S> Output type.
 */
public interface IMap<T, S> {
    S map(T data, IProgressReporter reporter, CancellationToken token);
}
