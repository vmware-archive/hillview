package org.hiero.sketch.dataset.api;

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
     * @return The result of the transformation.
     */
    S apply(T data);
}
