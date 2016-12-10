package org.hiero.sketch.dataset.api;

import rx.Observable;

import java.io.Serializable;

/**
 * A closure that runs a computation on an object of type T
 * and returns an object of type S.
 * @param <T> Input type.
 * @param <S> Output type.
 */
public interface IMap<T, S> extends Serializable {
    /**
     * Apply a transformation to the data.
     * @param data Data to transform.
     * @return A sequence of partial results containing the results of the transformation.
     * Only one of the partial results will contain the result of the map computation,
     * all the other ones will contain nulls.
     */
    Observable<PartialResult<S>> apply(T data);
}
