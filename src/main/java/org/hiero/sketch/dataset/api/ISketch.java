package org.hiero.sketch.dataset.api;

import rx.Observable;

import java.io.Serializable;

/**
 * Describes a sketch computation on a dataset of type T.
 * @param <T> Input data type.
 * @param <R> Output data type.
 */
public interface ISketch<T, R> extends Serializable, IMonoid<R> {
    R zero();
    R add(R left, R right);
    Observable<PartialResult<R>> create(T data);
}
